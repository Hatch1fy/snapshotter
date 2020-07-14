# Snapshotter

Snapshotter is a database snapshot utility which snapshots and truncates for the configured values.

# Usage
```go
package main

import (
	"fmt"
	"log"
	"os"
	"path"
	"time"

	"github.com/gdbu/snapshotter"
	"github.com/gdbu/snapshotter/backends"
	"github.com/gdbu/snapshotter/frontends"

	"github.com/boltdb/bolt"
)

const (
	frontendDir = "./frontend"
	backendDir  = "./backend"
)

func main() {
	var (
		s   *snapshotter.Snapshotter
		db  *bolt.DB
		err error
	)

	// Create bolt database
	if db, err = createBoltDB(); err != nil {
		out.Errorf("error during Init: %v", err)
        return
	}

	// Populate bolt database
	if err = populateValues(db); err != nil {
		out.Errorf("error during Init: %v", err)
        return
	}

	// Initialize bolt front-end
	fe := frontends.NewBolt(db)

	// Initialize a new file backend
	be := backends.NewFilebackend(backendDir)

	// Create new configuration
	cfg := snapshotter.NewConfig("data", "db")
	// Interval represents our snapshot interval value. This is how often we will snapshot data. We will set
	// our configuration so that the snapshotter takes a snapshot once a second.
	cfg.Interval = snapshotter.Second
	// Truncate represents our snapshot truncate value. This determines which time value our snapshots will
	// truncate to. We will set our configuration so that the snapshotter truncates our time value to the minute.
	cfg.Truncate = snapshotter.Minute

	if s, err = snapshotter.New(fe, be, cfg); err != nil {
		out.Errorf("error during Init: %v", err)
        return
	}
	defer s.Close()

	// Give snapshotter time to take some snapshots
	time.Sleep(5 * time.Second)

	var latest string
	// Get latest key
	if latest, err = s.LatestKey(); err != nil {
		out.Errorf("error during Init: %v", err)
        return
	}

	fmt.Printf("Our latest key was: %s\n", latest)
}

func createBoltDB() (db *bolt.DB, err error) {
	// Ensure our frontend test directory has been created
	if err = os.MkdirAll(frontendDir, 0744); err != nil {
		out.Errorf("error during Init: %v", err)
        return
	}

	// Open a bolt database within the frontend test directory with the name of "bolt.db"
	return bolt.Open(path.Join(frontendDir, "bolt.db"), 0744, nil)
}

func populateValues(db *bolt.DB) (err error) {
	bucketKey := []byte("main")
	key := []byte("greeting")
	value := []byte("hello world")

	// Update bolt database
	err = db.Update(func(txn *bolt.Tx) (err error) {
		var bkt *bolt.Bucket
		// Create bucket with the key equaling our bucketKey
		if bkt, err = txn.CreateBucketIfNotExists(bucketKey); err != nil {
			return
		}

		// Put our value to the database
		return bkt.Put(key, value)
	})

	return
}

```