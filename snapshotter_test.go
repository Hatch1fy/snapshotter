package snapshotter

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/Hatch1fy/snapshotter/backends"
	"github.com/Hatch1fy/snapshotter/frontends"
	"github.com/boltdb/bolt"
	"github.com/missionMeteora/toolkit/errors"
)

func TestSnapshotter(t *testing.T) {
	be := backends.NewFilebackend("./testing_backend")
	testBolt(t, be)
}

func testBolt(t *testing.T, be Backend) {
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

	bucketKey := []byte("test")
	key := []byte("key")
	value := []byte("1")

	if err = db.Update(func(txn *bolt.Tx) (err error) {
		var bkt *bolt.Bucket
		if bkt, err = txn.CreateBucketIfNotExists(bucketKey); err != nil {
			return
		}

		return bkt.Put(key, value)
	}); err != nil {
		t.Fatal(err)
	}

	// Initialize bolt front-end
	fe := frontends.NewBolt(db)

	// Call testSnapshotter and pass confirmation func
	testSnapshotter(t, fe, be, func(r io.Reader) (err error) {
		var filename string
		if filename, err = newTestTmpFile(r); err != nil {
			return
		}
		defer os.Remove(filename)

		return confirmBolt(filename, bucketKey, key, value)
	})
}

func newTestTmpFile(r io.Reader) (filename string, err error) {
	var f *os.File
	if f, err = ioutil.TempFile("", "snapshotter_test"); err != nil {
		return
	}

	filename = f.Name()
	_, err = io.Copy(f, r)
	f.Close()
	return
}

func confirmBolt(filename string, bucketKey, key, intendedValue []byte) (err error) {
	var db *bolt.DB
	if db, err = bolt.Open(filename, 0744, nil); err != nil {
		return
	}

	return db.View(func(txn *bolt.Tx) (err error) {
		var bkt *bolt.Bucket
		if bkt = txn.Bucket(bucketKey); bkt == nil {
			return errors.Error("bucket does not exist when it should")
		}

		var bs []byte
		if bs = bkt.Get(key); !bytes.Equal(bs, intendedValue) {
			return fmt.Errorf("invalid value, expected \"%s\" and received \"%s\"", "1", string(bs))
		}

		return
	})
}

func testSnapshotter(t *testing.T, fe Frontend, be Backend, confirm func(io.Reader) error) {
	var (
		s   *Snapshotter
		err error
	)

	// Initialize configuration
	cfg := NewConfig("test", "db")
	// Set interval to one second
	cfg.Interval = Second
	// Set truncate to one sec
	cfg.Truncate = Second
	// Set data directory
	cfg.DataDir = "./testing_data"

	defer os.RemoveAll("./testing_data")

	// Initialize a new instance of Snapshotter
	if s, err = New(fe, be, cfg); err != nil {
		t.Fatal(err)
	}
	// Defer the closing of Snapshotter
	defer s.Close()

	time.Sleep(time.Second * 5)

	var lastKey string
	// Get the latest key
	if lastKey, err = s.LastKey(); err != nil {
		t.Fatal(err)
	}

	// Call load and pass the reader to confirmation function
	if err = s.Load(lastKey, confirm); err != nil {
		t.Fatal(err)
	}
}
