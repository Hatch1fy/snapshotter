package snapshotter

import (
	"fmt"
	"sync"
	"time"

	"github.com/PathDNA/atoms"
	"github.com/missionMeteora/toolkit/errors"
)

const (
	// ErrInvalidTruncate is returned when an invalid truncate duration is set
	ErrInvalidTruncate = errors.Error("invalid truncate duration, must select time.Hour, time.Minute, or time.Second")
	// ErrInvalidInterval is returned when an invalid interval duration is set
	ErrInvalidInterval = errors.Error("invalid interval duration, must be greater than or equal to one second")
)

// New returns a new instance of snapshotter
func New(sn Snapshottee, be Backend, cfg Config) (sp *Snapshotter, err error) {
	var s Snapshotter
	s.sn = sn
	s.be = be
	s.cfg = cfg

	if !isValidTruncate(cfg.Truncate) {
		err = ErrInvalidTruncate
		return
	}

	go s.loop(s.cfg.Interval)
	sp = &s
	return
}

// Snapshotter will manage a snapshotting service
type Snapshotter struct {
	mu sync.RWMutex

	sn  Snapshottee
	be  Backend
	cfg Config

	trunc time.Duration

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

// snapshot will call a new Writer and Snapshottee then copy to the Writer
func (s *Snapshotter) snapshot() (err error) {
	// Get new key
	key := getKey(s.cfg.Name, s.cfg.Extension, s.trunc)
	// Attempt to write to our Writee
	return s.be.WriteTo(key, s.sn.Copy)
}

// Snapshot will call a new Writer and Snapshottee then copy to the Writer
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
