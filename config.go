package snapshotter

import (
	"time"

	"github.com/Hatch1fy/errors"
)

// NewConfig will return a new default Config with the given name and extension
func NewConfig(name, ext string) (cfg Config) {
	cfg.Name = name
	cfg.Extension = ext
	cfg.Interval = Minute
	cfg.Truncate = Hour
	return
}

// Config are the basic configuration settings for snapshotter
type Config struct {
	Name      string
	Extension string
	DataDir   string
	Interval  time.Duration
	Truncate  time.Duration
}

// Validate will validate a Config
func (c *Config) Validate() (err error) {
	var errs errors.ErrorList
	if !isValidTruncate(c.Truncate) {
		errs.Push(ErrInvalidTruncate)
	}

	if c.Interval < Second {
		errs.Push(ErrInvalidInterval)
	}

	if len(c.DataDir) == 0 {
		errs.Push(ErrInvalidDataDirectory)
	}

	return errs.Err()
}
