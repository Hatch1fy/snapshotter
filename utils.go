package snapshotter

import (
	"fmt"
	"io"
	"strconv"
	"time"
)

const (
	// Second represents a second
	Second = time.Second
	// Minute represents a minute
	Minute = time.Minute
	// Hour represents an hour
	Hour = time.Hour
	// Day represents a day
	Day = Hour * 24
	// Month represents a month
	Month = Day * 30
	// Year represents a year
	Year = Day * 365
)

func getKey(truncate time.Duration) (key string) {
	// Get current time
	now := time.Now()
	// Truncate time to truncate value
	truncated := getTruncated(now, truncate)
	// Get the nano Unix timestamp
	unix := truncated.Unix()
	// Convert the timestamp to a string
	return strconv.FormatInt(unix, 10)
}

func getTruncated(t time.Time, truncate time.Duration) (truncated time.Time) {
	switch truncate {
	case Year:
		return time.Date(t.Year(), time.January, 0, 0, 0, 0, 0, t.Location())
	case Month:
		return time.Date(t.Year(), t.Month(), 0, 0, 0, 0, 0, t.Location())
	case Day:
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location())
	case Hour:
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location())
	case Minute:
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), 0, 0, t.Location())
	case Second:
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), 0, t.Location())

	default:
		// This should have been caught by New call. This panic should NEVER happen
		panic(fmt.Sprintf("invalid truncate provided: %v\n", truncate))
	}
}

// Snapshottee is the interface for values which can be used for snapshots
type Snapshottee interface {
	Copy(w io.Writer) error
}

// Backend is the interface for values which can be stored and retrieved
type Backend interface {
	WriteTo(key string, fn func(io.Writer) error) error
	//	Get(key string) (io.Reader, error)
	//	GetAt(key string, timestamp int64) (io.Reader, error)
}
