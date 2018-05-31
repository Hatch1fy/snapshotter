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

	"github.com/Hatch1fy/snapshotter"
	"github.com/Hatch1fy/snapshotter/backends"
	"github.com/Hatch1fy/snapshotter/frontends"

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
		log.Fatal(err)
	}

	// Populate bolt database
	if err = populateValues(db); err != nil {
		log.Fatal(err)
	}

	// Initialize bolt front-end
	fe := frontends.NewBolt(db)

	// Initialize a new file backend
	be := backends.NewFilebackend(backendDir)

	// Create new configuration
	cfg := snapshotter.NewConfig("data", "db")
	// Set interval of one second
	cfg.Interval = snapshotter.Second
	// Set truncate of one second
	cfg.Truncate = snapshotter.Second

	if s, err = snapshotter.New(fe, be, cfg); err != nil {
		log.Fatal(err)
	}
	defer s.Close()

	// Give snapshotter time to take some snapshots
	time.Sleep(5 * time.Second)

	var latest string
	// Get latest key
	if latest, err = s.LatestKey(); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Our latest key was: %s\n", latest)
}

func createBoltDB() (db *bolt.DB, err error) {
	// Ensure our frontend test directory has been created
	if err = os.MkdirAll(frontendDir, 0744); err != nil {
		log.Fatal(err)
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