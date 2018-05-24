package snapshottees

import (
	"io"

	"github.com/boltdb/bolt"
)

// NewBolt returns a new bolt.DB Snapshottee layer
func NewBolt(db *bolt.DB) *Bolt {
	var b Bolt
	b.db = db
	return &b
}

// Bolt is a Snapshottee layer for bolt.DB
type Bolt struct {
	db *bolt.DB
}

// Copy will copy to an io.Writer
func (b *Bolt) Copy(w io.Writer) (err error) {
	return b.db.View(func(txn *bolt.Tx) (err error) {
		return txn.Copy(w)
	})
}
