package snapshotter

import (
	"os"
	"testing"
	"time"

	"github.com/Hatch1fy/snapshotter/backends"
	"github.com/Hatch1fy/snapshotter/frontends"
	"github.com/boltdb/bolt"
)

func TestSnapshotter(t *testing.T) {
	var (
		db  *bolt.DB
		err error
	)

	if err = os.MkdirAll("./testing_frontend", 0744); err != nil {
		t.Fatal(err)
	}

	if db, err = bolt.Open("./testing_frontend/bolt.db", 0744, nil); err != nil {
		t.Fatal(err)
	}

	fe := frontends.NewBolt(db)
	be := backends.NewFilebackend("./testing_backend")

	if err = db.Update(func(txn *bolt.Tx) (err error) {
		var bkt *bolt.Bucket
		if bkt, err = txn.CreateBucketIfNotExists([]byte("test")); err != nil {
			return
		}

		return bkt.Put([]byte("key"), []byte("1"))
	}); err != nil {
		t.Fatal(err)
	}

	if err = testSnapshotter(fe, be); err != nil {
		t.Fatal(err)
	}
}

func testSnapshotter(fe Frontend, be Backend) (err error) {
	var s *Snapshotter
	// Initialize configuration
	cfg := NewConfig("test", "db")
	// Set interval to one second
	cfg.Interval = Second
	// Set truncate to one sec
	cfg.Truncate = Second

	// Initialize a new instance of Snapshotter
	if s, err = New(fe, be, cfg); err != nil {
		return
	}
	// Defer the closing of Snapshotter
	defer s.Close()

	time.Sleep(time.Second * 5)
	return
}
