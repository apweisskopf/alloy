package positions

// This code is copied from Promtail. The positions package allows logging
// components to keep track of read file offsets on disk and continue from the
// same place in case of a restart.

import (
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

// Config describes where to get position information from.
type Config struct {
	SyncPeriod    time.Duration
	PositionsFile string
}

// PositionsFile tracks how far through each file we've read.
type PositionsFile struct {
	logger    log.Logger
	cfg       Config
	mut       sync.RWMutex
	positions map[Entry]string
	quit      chan struct{}
	done      chan struct{}
}

// New makes a new Positions.
func New(logger log.Logger, cfg Config) (Positions, error) {
	positionData, err := readPositionsFile(cfg, logger)
	if err != nil {
		return nil, err
	}

	p := &PositionsFile{
		logger:    logger,
		cfg:       cfg,
		positions: positionData,
		quit:      make(chan struct{}),
		done:      make(chan struct{}),
	}

	go p.run()
	return p, nil
}

func (p *PositionsFile) Stop() {
	close(p.quit)
	<-p.done
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
	return p.cfg.SyncPeriod
}

func (p *PositionsFile) run() {
	defer func() {
		p.save()
		level.Debug(p.logger).Log("msg", "positions saved")
		close(p.done)
	}()

	ticker := time.NewTicker(p.cfg.SyncPeriod)
	for {
		select {
		case <-p.quit:
			return
		case <-ticker.C:
			p.save()
			p.cleanup()
		}
	}
}

func (p *PositionsFile) save() {
	p.mut.Lock()
	positions := make(map[Entry]string, len(p.positions))
	maps.Copy(positions, p.positions)
	p.mut.Unlock()

	if err := writePositionFile(p.cfg.PositionsFile, positions); err != nil {
		level.Error(p.logger).Log("msg", "error writing positions file", "error", err)
	}

	level.Debug(p.logger).Log("msg", "positions saved")
}

func (p *PositionsFile) cleanup() {
	p.mut.Lock()
	defer p.mut.Unlock()
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

func readPositionsFile(cfg Config, logger log.Logger) (map[Entry]string, error) {
	cleanfn := filepath.Clean(cfg.PositionsFile)
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
