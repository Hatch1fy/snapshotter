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
)

// New returns a new instance of snapshotter
func New(sn Snapshottee, be Backend, interval int, truncate time.Duration) (sp *Snapshotter, err error) {
	var s Snapshotter
	s.sn = sn
	s.be = be

	switch truncate {
	case time.Hour:
	case time.Minute:
	case time.Second:
	default:
		err = ErrInvalidTruncate
		return
	}

	s.trunc = truncate

	go s.loop(interval)
	sp = &s
	return
}

// Snapshotter will manage a snapshotting service
type Snapshotter struct {
	mu sync.RWMutex

	sn Snapshottee
	be Backend

	trunc time.Duration

	closed atoms.Bool
}

// loop will loop snapshots on a provided interval (in seconds)
func (s *Snapshotter) loop(interval int) {
	var err error
	// Set interval duration as our interval converted to time.Second
	intervalDuration := time.Second * time.Duration(interval)
	// Run loop as long as our service hasn't closed
	for err != errors.ErrIsClosed {
		// We sleep first because we want to wait for the interval duration before snapshotting.
		time.Sleep(intervalDuration)
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
	key := getKey(s.trunc)
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
