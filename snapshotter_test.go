package snapshotter

import (
	"os"
	"testing"
	"time"

	"github.com/Hatch1fy/snapshotter/backends"
	"github.com/Hatch1fy/snapshotter/snapshottees"
	"github.com/boltdb/bolt"
)

func TestSnapshotter(t *testing.T) {
	var (
		s   *Snapshotter
		db  *bolt.DB
		err error
	)

	if err = os.MkdirAll("./testing_snapshottee", 0744); err != nil {
		t.Fatal(err)
	}

	if db, err = bolt.Open("./testing_snapshottee/bolt.db", 0744, nil); err != nil {
		t.Fatal(err)
	}

	sn := snapshottees.NewBolt(db)
	fb := backends.NewFilebackend("./testing_backend")

	if s, err = New(sn, fb, 1, time.Second); err != nil {
		t.Fatal(err)
	}
	//defer s.Close()

	db.Update(func(txn *bolt.Tx) (err error) {
		var bkt *bolt.Bucket
		if bkt, err = txn.CreateBucketIfNotExists([]byte("test")); err != nil {
			return
		}

		return bkt.Put([]byte("key"), []byte("1"))
	})

	time.Sleep(time.Second * 5)

	s.Close()
}
