package snapshotter

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"

	"github.com/Hatch1fy/errors"
	"github.com/Hatch1fy/snapshotter/backends"
	"github.com/Hatch1fy/snapshotter/frontends"
	"github.com/boltdb/bolt"
)

const (
	backendTestDir  = "./testing_backend"
	frontendTestDir = "./testing_frontend"
)

func TestSnapshotter(t *testing.T) {
	// Defer the removal of our backend test directory
	defer os.RemoveAll(backendTestDir)
	// Initialize a new file backend
	be := backends.NewFile(backendTestDir)
	// Perform bolt test with Filebackend as the backend
	testBolt(t, be)
}

func testBolt(t *testing.T, be Backend) {
	var (
		db  *bolt.DB
		err error
	)

	// Defer the removal of our frontend test directory
	defer os.RemoveAll(frontendTestDir)

	// Ensure our frontend test directory has been created
	if err = os.MkdirAll(frontendTestDir, 0744); err != nil {
		t.Fatal(err)
	}

	// Open a bolt database within the frontend test directory with the name of "bolt.db"
	if db, err = bolt.Open(path.Join(frontendTestDir, "bolt.db"), 0744, nil); err != nil {
		t.Fatal(err)
	}

	bucketKey := []byte("test")
	key := []byte("key")
	value := []byte("1")

	// Update bolt database
	if err = db.Update(func(txn *bolt.Tx) (err error) {
		var bkt *bolt.Bucket
		// Create bucket with the key equaling our bucketKey
		if bkt, err = txn.CreateBucketIfNotExists(bucketKey); err != nil {
			return
		}

		// Put our value to the database
		return bkt.Put(key, value)
	}); err != nil {
		t.Fatal(err)
	}

	// Initialize bolt front-end
	fe := frontends.NewBolt(db)

	// Call testSnapshotter and pass confirmation func
	testSnapshotter(t, fe, be, func(r io.Reader) (err error) {
		var filename string
		// Create new temp file and get the filename
		if filename, err = newTestTmpFile(r); err != nil {
			return
		}
		// Defer the removal of our temp file
		defer os.Remove(filename)

		// Call confirmBoltTest which will ensure our inbound database has the proper values
		return confirmBoltTest(filename, bucketKey, key, value)
	})
}

// newTestTmpFile will create a new temp file and return it's associated filename
func newTestTmpFile(r io.Reader) (filename string, err error) {
	var f *os.File
	// Create temp file
	if f, err = ioutil.TempFile("", "snapshotter_test"); err != nil {
		// Error encountered while creating, return
		return
	}

	// Set filename return value
	filename = f.Name()

	// Attempt to copy from reader to temp file
	_, err = io.Copy(f, r)

	// Close file
	f.Close()

	if err != nil {
		// Error encountered, remove file and set filename value to empty
		os.Remove(filename)
		filename = ""
	}

	return
}

// confirmBoltTest will ensure the database values are correct
func confirmBoltTest(filename string, bucketKey, key, intendedValue []byte) (err error) {
	var db *bolt.DB
	// Open bolt database at provided filename
	if db, err = bolt.Open(filename, 0744, nil); err != nil {
		return
	}

	return db.View(func(txn *bolt.Tx) (err error) {
		var bkt *bolt.Bucket
		// Attempt to get bucket at the bucketKey
		if bkt = txn.Bucket(bucketKey); bkt == nil {
			// Bucket does not exist, return
			return errors.Error("bucket does not exist when it should")
		}

		var bs []byte
		// Get the bytes for the given key and compare them to the intended value
		if bs = bkt.Get(key); !bytes.Equal(bs, intendedValue) {
			// Bytes are not correct, return
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

	// Initialize a new instance of Snapshotter
	if s, err = New(fe, be, cfg); err != nil {
		t.Fatal(err)
	}
	// Defer the closing of Snapshotter
	defer s.Close()

	// Sleep for 5 seconds
	time.Sleep(time.Second * 5)

	var latest string
	// Get the latest key
	if latest, err = s.LatestKey(); err != nil {
		t.Fatal(err)
	}

	// Call load and pass the reader to confirmation function
	if err = s.Load(latest, confirm); err != nil {
		t.Fatal(err)
	}
}
