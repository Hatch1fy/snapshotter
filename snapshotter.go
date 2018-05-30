package snapshotter

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/Hatch1fy/mmapstore"
	"github.com/PathDNA/atoms"
	"github.com/missionMeteora/toolkit/errors"
)

const (
	// ErrInvalidTruncate is returned when an invalid truncate duration is set
	ErrInvalidTruncate = errors.Error("invalid truncate duration, must select time.Hour, time.Minute, or time.Second")
	// ErrInvalidInterval is returned when an invalid interval duration is set
	ErrInvalidInterval = errors.Error("invalid interval duration, must be greater than or equal to one second")
	// ErrInvalidDataDirectory is returned when an invalid data directory is set
	ErrInvalidDataDirectory = errors.Error("invalid data directory, cannot be empty")
)

// New returns a new instance of snapshotter
func New(fe Frontend, be Backend, cfg Config) (sp *Snapshotter, err error) {
	var s Snapshotter
	s.fe = fe
	s.be = be
	s.cfg = cfg

	if !isValidTruncate(cfg.Truncate) {
		err = ErrInvalidTruncate
		return
	}

	// Create an example reference key
	referenceKey := getKey(s.cfg.Name, s.cfg.Extension, s.cfg.Truncate)
	// Get the length of the key
	keyLen := int64(len(referenceKey))

	// Initialize a new instance of mmapstore
	if s.lastKey, err = mmapstore.New(cfg.Name+".sref", cfg.DataDir, keyLen); err != nil {
		return
	}

	// Begin snapshot loop
	go s.loop(s.cfg.Interval)
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

	// Last key store
	lastKey *mmapstore.MMapStore
	// Closed state
	closed atoms.Bool
}

// loop will loop snapshots on a provided interval (in seconds)
func (s *Snapshotter) loop(interval time.Duration) {
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

// snapshot will write to our back-end from our front-end
func (s *Snapshotter) snapshot() (err error) {
	// Get new key
	key := getKey(s.cfg.Name, s.cfg.Extension, s.cfg.Truncate)

	// Attempt to write to our Writee
	if err = s.be.WriteTo(key, s.fe.Copy); err != nil {
		return
	}

	return s.setLatest(key)
}

func (s *Snapshotter) getLatest() (key string, err error) {
	// View latest key's current bytes
	err = s.be.ReadFrom("latest.txt", func(r io.Reader) (err error) {
		buf := bytes.NewBuffer(nil)
		if _, err = io.Copy(buf, r); err != nil {
			return
		}

		key = buf.String()
		return
	})

	return
}

func (s *Snapshotter) setLatest(key string) (err error) {
	err = s.be.WriteTo("latest.txt", func(w io.Writer) (err error) {
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
