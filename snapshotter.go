package snapshotter

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/hatchify/atoms"
	"github.com/hatchify/errors"
)

const (
	// ErrInvalidTruncate is returned when an invalid truncate duration is set
	ErrInvalidTruncate = errors.Error("invalid truncate duration, must select time.Hour, time.Minute, or time.Second")
	// ErrInvalidInterval is returned when an invalid interval duration is set
	ErrInvalidInterval = errors.Error("invalid interval duration, must be greater than or equal to one second")
	// ErrInvalidName is returned when an invalid name is set
	ErrInvalidName = errors.Error("invalid name, cannot be empty")
	// ErrInvalidExtension is returned when an invalid extension is set
	ErrInvalidExtension = errors.Error("invalid extension, cannot be empty")
	// ErrInvalidKey is returned when an invalid key is attempted to be parsed
	ErrInvalidKey = errors.Error("provided key has an invalid number of delimiters, cannot parse")
	// ErrIsLatestKey is returned when a latest key is attempted to be parsed
	ErrIsLatestKey = errors.Error("cannot parse latest key")
)

// New returns a new instance of snapshotter
func New(fe Frontend, be Backend, cfg Config) (sp *Snapshotter, err error) {
	var s Snapshotter
	// Validate the inbound configuration
	if err = cfg.Validate(); err != nil {
		return
	}

	s.fe = fe
	s.be = be
	s.cfg = cfg

	// Begin snapshot loop
	go s.snapshotLoop(s.cfg.Interval)
	// Begin purge loop
	go s.purgeLoop(Hour)
	// Assign snapshotter pointer as a reference to our snapshotter struct
	sp = &s
	return
}

// Snapshotter will manage a snapshotting service
type Snapshotter struct {
	mu sync.RWMutex

	fe  Frontend
	be  Backend
	cfg Config

	// Closed state
	closed atoms.Bool
}

// snapshotLoop will continuously snapshot on a provided interval (in seconds)
func (s *Snapshotter) snapshotLoop(interval time.Duration) {
	var err error
	// Run loop as long as our service hasn't closed
	for err != errors.ErrIsClosed {
		// We sleep first because we want to wait for the interval duration before snapshotting.
		time.Sleep(interval)

		// Attempt to snapshot
		if err = s.snapshot(); err != nil {
			fmt.Printf("Error encountered snapshotting: %v\n", err)
		}
	}

	return
}

// purgeLoop will continuously purge  on a provided interval (in seconds)
func (s *Snapshotter) purgeLoop(interval time.Duration) {
	var err error
	// Run loop as long as our service hasn't closed
	for err != errors.ErrIsClosed {
		// Attempt to purge
		if err = s.purge(); err != nil {
			fmt.Printf("Error encountered purging: %v\n", err)
		}

		// We sleep after purging so we can ensure we are purged on start
		time.Sleep(interval)
	}

	return
}

// snapshot will write to our back-end from our front-end
func (s *Snapshotter) snapshot() (err error) {
	// Get new key
	key := getKey(s.cfg.Name, s.cfg.Extension, s.cfg.Truncate)

	// Attempt to write to our Writee
	if err = s.be.WriteTo(key, s.fe.Copy); err != nil {
		// Error encountered while writing, return
		return
	}

	// Set our latest key value
	return s.setLatest(key)
}

// purge delete entries older than the TTL
func (s *Snapshotter) purge() (err error) {
	var keys []string
	if keys, err = s.be.List(s.cfg.Name, "", 1000); err != nil {
		return
	}

	// Get cutoff timestamp
	cutoff := time.Now().Add(-s.cfg.TTL).Unix()

	// Iterate through returned keys
	for _, key := range keys {
		if err = s.remove(key, cutoff); err != nil {
			return
		}
	}

	return
}

func (s *Snapshotter) remove(key string, cutoff int64) (err error) {
	var unixTS int64
	if _, _, unixTS, err = parseKey(key); err != nil {
		if err == ErrIsLatestKey {
			return nil
		}

		return fmt.Errorf("error parsing key \"%s\": %v", key, err)
	}

	if unixTS > cutoff {
		return
	}

	if err = s.be.Delete(key); err != nil {
		return fmt.Errorf("error deleting \"%s\": %v", key, err)
	}

	return
}

func (s *Snapshotter) getLatest() (key string, err error) {
	// View latest key's current bytes
	err = s.be.ReadFrom(s.cfg.Name+".latest.txt", func(r io.Reader) (err error) {
		// Create buffer
		buf := bytes.NewBuffer(nil)
		// Copy reader bytes to buffer
		if _, err = io.Copy(buf, r); err != nil {
			// Error encountered while copying, return
			return
		}

		// Set key as the string output of our buffer
		key = buf.String()
		return
	})

	return
}

func (s *Snapshotter) setLatest(key string) (err error) {
	// Set latest key's current bytes
	err = s.be.WriteTo(s.cfg.Name+".latest.txt", func(w io.Writer) (err error) {
		// Write key as bytes
		_, err = w.Write([]byte(key))
		return
	})

	return
}

// Load will load a reader from the given key
func (s *Snapshotter) Load(key string, fn func(io.Reader) error) (err error) {
	// Ensure our service hasn't been closed
	if s.closed.Get() {
		// Service has been closed, return
		return errors.ErrIsClosed
	}

	// Read from back-end
	return s.be.ReadFrom(key, fn)
}

// Snapshot will call snapshot under the protection of a write-lock
func (s *Snapshotter) Snapshot() (err error) {
	// Ensure our service hasn't been closed
	if s.closed.Get() {
		// Service has been closed, return
		return errors.ErrIsClosed
	}

	// Acquire mutex lock
	s.mu.Lock()
	// Defer releasing of the mutex lock
	defer s.mu.Unlock()
	// Attempt to snapshot
	return s.snapshot()
}

// LatestKey will return the last key saved
func (s *Snapshotter) LatestKey() (key string, err error) {
	// Ensure our service hasn't been closed
	if s.closed.Get() {
		// Service has been closed, return
		err = errors.ErrIsClosed
		return
	}

	return s.getLatest()
}

// Close will close the Snapshotter
func (s *Snapshotter) Close() (err error) {
	if !s.closed.Set(true) {
		return errors.ErrIsClosed
	}

	// Acquire mutex lock
	s.mu.Lock()
	// Defer releasing of the mutex lock
	defer s.mu.Unlock()
	// Attempt to snapshot once more before closing
	return s.snapshot()
}
