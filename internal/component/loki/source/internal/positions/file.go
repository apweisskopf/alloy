package positions

// This code is copied from Promtail. The positions package allows logging
// components to keep track of read file offsets on disk and continue from the
// same place in case of a restart.

import (
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"gopkg.in/yaml.v2"

	"github.com/grafana/alloy/internal/runtime/logging/level"
	"github.com/grafana/alloy/syntax"
)

const (
	cursorKeyPrefix  = "cursor-"
	journalKeyPrefix = "journal-"
)

// CursorKey returns a key that can be saved as a cursor that is never deleted.
func CursorKey(key string) string {
	return cursorKeyPrefix + key
}

// Entry describes a positions file entry consisting of an absolute file path and
// the matching label set.
// An entry expects the string representation of a LabelSet or a Labels slice
// so that it can be utilized as a YAML key. The caller should make sure that
// the order and structure of the passed string representation is reproducible,
// and maintains the same format for both reading and writing from/to the
// positions file.
type Entry struct {
	Path   string `yaml:"path"`
	Labels string `yaml:"labels"`
}

// File is the format for the positions data on disk.
type File struct {
	Positions map[Entry]string `yaml:"positions"`
}

var (
	_ syntax.Defaulter = (*Config)(nil)
	_ syntax.Validator = (*Config)(nil)
)

// Config describes where to get position information from.
type Config struct {
	SyncPeriod time.Duration `alloy:"sync_period,attr,optional"`
}

func (c *Config) Validate() error {
	if c.SyncPeriod <= 0 {
		return errors.New("sync_period must be greater than 0")
	}
	return nil
}

func (c *Config) SetToDefault() {
	c.SyncPeriod = 10 * time.Second
}

// PositionsFile tracks how far through each file we've read.
type PositionsFile struct {
	logger    log.Logger
	cfg       Config
	path      string
	mut       sync.RWMutex
	positions map[Entry]string
	quit      chan struct{}
	done      chan struct{}
}

// New makes a new Positions.
func New(logger log.Logger, path string, cfg Config) (Positions, error) {
	positionData, err := readPositionsFile(path)
	if err != nil {
		return nil, err
	}

	p := &PositionsFile{
		logger:    logger,
		cfg:       cfg,
		path:      path,
		positions: positionData,
		quit:      make(chan struct{}),
		done:      make(chan struct{}),
	}

	go p.run()
	return p, nil
}

func (p *PositionsFile) Update(cfg Config) {
	p.mut.RLock()
	if cfg.SyncPeriod != p.cfg.SyncPeriod {
		p.mut.RUnlock()
		p.mut.Lock()
		defer p.mut.Unlock()
		p.cfg = cfg
		return
	}
	p.mut.RUnlock()
}

func (p *PositionsFile) Get(path, labels string) (int64, error) {
	p.mut.RLock()
	defer p.mut.RUnlock()
	pos, ok := p.positions[Entry{path, labels}]
	if !ok {
		return 0, nil
	}
	return strconv.ParseInt(pos, 10, 64)
}

func (p *PositionsFile) GetString(path, labels string) string {
	p.mut.RLock()
	defer p.mut.RUnlock()
	return p.positions[Entry{path, labels}]
}

func (p *PositionsFile) Put(path, labels string, pos int64) {
	p.PutString(path, labels, strconv.FormatInt(pos, 10))
}

func (p *PositionsFile) PutString(path, labels string, pos string) {
	p.mut.Lock()
	defer p.mut.Unlock()
	p.positions[Entry{path, labels}] = pos
}

func (p *PositionsFile) Remove(path, labels string) {
	p.mut.Lock()
	defer p.mut.Unlock()
	p.remove(path, labels)
}

func (p *PositionsFile) remove(path, labels string) {
	delete(p.positions, Entry{path, labels})
}

func (p *PositionsFile) SyncPeriod() time.Duration {
	p.mut.RLock()
	defer p.mut.RUnlock()
	return p.cfg.SyncPeriod
}

func (p *PositionsFile) Stop() {
	close(p.quit)
	<-p.done
}

func (p *PositionsFile) run() {
	defer func() {
		p.mut.Lock()
		defer p.mut.Unlock()
		p.save()
		level.Debug(p.logger).Log("msg", "positions saved")
		close(p.done)
	}()

	p.mut.RLock()
	ticker := time.NewTicker(p.cfg.SyncPeriod)
	p.mut.RUnlock()

	for {
		select {
		case <-p.quit:
			return
		case <-ticker.C:
			// We only need read lock for save and reset call because we only read state.
			p.mut.RLock()
			p.save()
			ticker.Reset(p.cfg.SyncPeriod)
			p.mut.RUnlock()

			// We need write lock for cleanup because it will mutate state.
			p.mut.Lock()
			p.cleanup()
			p.mut.Unlock()
		}
	}
}

func (p *PositionsFile) save() {
	positions := make(map[Entry]string, len(p.positions))
	maps.Copy(positions, p.positions)

	if err := writePositionFile(p.path, positions); err != nil {
		level.Error(p.logger).Log("msg", "error writing positions file", "error", err)
	}

	level.Debug(p.logger).Log("msg", "positions saved")
}

func (p *PositionsFile) cleanup() {
	toRemove := []Entry{}
	for k := range p.positions {
		// If the position file is prefixed with cursor, it's a
		// cursor and not a file on disk.
		// We still have to support journal files, so we keep the previous check to avoid breaking change.
		if strings.HasPrefix(k.Path, cursorKeyPrefix) || strings.HasPrefix(k.Path, journalKeyPrefix) {
			continue
		}

		if _, err := os.Stat(k.Path); err != nil {
			if os.IsNotExist(err) {
				// File no longer exists.
				toRemove = append(toRemove, k)
			} else {
				// Can't determine if file exists or not, some other error.
				level.Warn(p.logger).Log("msg", "could not determine if log file "+
					"still exists while cleaning positions file", "error", err)
			}
		}
	}
	for _, tr := range toRemove {
		p.remove(tr.Path, tr.Labels)
	}
}

func readPositionsFile(path string) (map[Entry]string, error) {
	cleanfn := filepath.Clean(path)
	buf, err := os.ReadFile(cleanfn)
	if err != nil {
		if os.IsNotExist(err) {
			return map[Entry]string{}, nil
		}
		return nil, err
	}

	var p File
	err = yaml.Unmarshal(buf, &p)
	if err != nil {
		return nil, fmt.Errorf("invalid yaml positions file [%s]: %v", cleanfn, err)
	}

	// p.Positions will be nil if the file exists but is empty
	if p.Positions == nil {
		p.Positions = map[Entry]string{}
	}

	return p.Positions, nil
}
