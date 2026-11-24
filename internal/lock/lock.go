package lock

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/gofrs/flock"
)

var ErrWriterHeld = errors.New("another NewsPulse writer holds the data directory lock")

// DirLock is the single-writer guard for a data directory. It must be taken by
// the daemon and by every CLI command that writes.
type DirLock struct {
	fl *flock.Flock
}

func Acquire(dataDir string) (*DirLock, error) {
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}
	path := filepath.Join(dataDir, "newspulse.lock")
	fl := flock.New(path)
	ok, err := fl.TryLock()
	if err != nil {
		return nil, fmt.Errorf("acquire lock %s: %w", path, err)
	}
	if !ok {
		return nil, ErrWriterHeld
	}
	return &DirLock{fl: fl}, nil
}

func (l *DirLock) Release() error {
	if l == nil || l.fl == nil {
		return nil
	}
	return l.fl.Unlock()
}

// TryAcquireFor is used for operations that must not wait indefinitely behind
// a backup pause.
func TryAcquireFor(dataDir string, timeout time.Duration) (*DirLock, error) {
	path := filepath.Join(dataDir, "newspulse.lock")
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return nil, err
	}
	fl := flock.New(path)
	deadline := time.Now().Add(timeout)
	for {
		ok, err := fl.TryLock()
		if err != nil {
			return nil, err
		}
		if ok {
			return &DirLock{fl: fl}, nil
		}
		if time.Now().After(deadline) {
			return nil, errors.New("timed out waiting for data directory lock")
		}
		time.Sleep(100 * time.Millisecond)
	}
}
