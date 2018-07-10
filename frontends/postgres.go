package frontends

import (
	"io"

	"github.com/Hatch1fy/pgutils"

	"sync/atomic"
)

// NewPostgres returns a new PostgresDB front-end layer
func NewPostgres(cfg pgutils.Config) *Postgres {
	var p Postgres
	p.cfg = cfg
	return &p
}

// Postgres is a front-end layer for PostgresDB
type Postgres struct {
	cfg pgutils.Config

	callback atomic.Value
}

// Copy will copy to an io.Writer
func (p *Postgres) Copy(w io.Writer) (err error) {
	if err = pgutils.Dump(p.cfg, w); err != nil {
		return
	}

	if cb, ok := p.callback.Load().(func()); ok {
		cb()
	}

	return
}
