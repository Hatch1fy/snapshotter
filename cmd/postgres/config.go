package main

import (
	"time"

	"github.com/BurntSushi/toml"
)

func newConfig(src string) (c Config, err error) {
	_, err = toml.DecodeFile(src, &c)
	return
}

// Config is the postgres snapshotter configuration
type Config struct {
	// Name of database
	Name string `toml:"name"`
	// Environment name (lowercase preferred)
	Environment string `toml:"environment"`
	// Target Amazon S3 bucket
	Bucket string `toml:"bucket"`
	// Interval in minutes
	Interval time.Duration `toml:"interval"`
}
