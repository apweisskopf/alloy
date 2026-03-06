package positions

import "time"

type Positions interface {
	Update(cfg Config)
	// GetString returns how far we've through a file as a string.
	// JournalTarget writes a journal cursor to the positions file, while
	// FileTarget writes an integer offset. Use Get to read the integer
	// offset.
	GetString(path, labels string) string
	// Get returns how far we've read through a file. Returns an error
	// if the value stored for the file is not an integer.
	Get(path, labels string) (int64, error)
	// PutString records (asynchronously) how far we've read through a file.
	// Unlike Put, it records a string offset and is only useful for
	// JournalTargets which doesn't have integer offsets.
	PutString(path, labels string, pos string)
	// Put records (asynchronously) how far we've read through a file.
	Put(path, labels string, pos int64)
	// Remove removes the position tracking for a filepath
	Remove(path, labels string)
	// SyncPeriod returns how often the positions file gets resynced
	SyncPeriod() time.Duration
	// Stop the Position tracker.
	Stop()
}
