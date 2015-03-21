/*
Testing out boltdb for the following use case:

* Load several million key/value pairs in as quickly as possible
* Data forms a graph, that will be searched using A*
* Load once, search many times
* Data is too big to be all be in memory

Issues so far:

* Writing to boltdb involves 2 writes to disk, so performance is terrible if writes aren't batched

Ideas to try out:

* Built test suite with a regular map  [DONE]
* Swap in boltdb backend and compare.  [DONE]
* Try out boltdb transaction coalescer [DONE]
  https://github.com/boltdb/coalescer
* Rerun on SSD                         [DONE]
* Separate test to measure how long it takes to read all the values back. [DONE]


Findings:

* Overhead of db.Update for single key/value write is massive.
  At 1 million keys per db.Update overhead  still 5x slower

coalescer -- Not working well even on an SSD, but works. Go back to home built solution.
 (Found issue with coalescer logic)

* Reading back, as expected is faster then writing.

number of entries: 5 Million
Write map test took: 5.528 s
Write bolt test took: 38.55 s
Write bolt/map: 7.0X
Read bolt test took: 15.99 s

bolt db file size: ~1GB


*/

package main

import (
	"encoding/json"
	"fmt"
	"github.com/boltdb/bolt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

// Interface used for testing
type db interface {
	Writer(key string, value []string)
	Flush()
}

type mapType struct {
	db map[string][]string
}

func (m *mapType) Writer(key string, value []string) {
	m.db[key] = value
}

func (m *mapType) Flush() {
}

func newMapType() *mapType {
	m := mapType{
		db: make(map[string][]string),
	}
	return &m
}

type boltType struct {
	Db        *bolt.DB
	buffer    map[string][]string
	batchSize int
}

func newBoltType(limit int) *boltType {
	db := prepBolt(limit)
	b := boltType{
		Db:     db,
		buffer: make(map[string][]string),
		// If batch is too things slow down
		batchSize: 10000,
	}
	return &b
}

func (mybolt *boltType) Writer(key string, value []string) {
	mybolt.buffer[key] = value
	if len(mybolt.buffer) > mybolt.batchSize {
		mybolt.Flush()
	}
}

func (mybolt *boltType) Flush() {
	err := mybolt.Db.Update(func(tx *bolt.Tx) error {
		//var err error
		b := tx.Bucket(bucket)
		for key, value := range mybolt.buffer {
			bytes, err := json.Marshal(value)
			if err != nil {
				return err
			}
			err = b.Put([]byte(key), bytes)
			delete(mybolt.buffer, key)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
	mybolt.Db.NoSync = true
}

var bucket = []byte("MyBucket")

func prepBolt(limit int) *bolt.DB {
	path := "my.db"
	// make sure we start from a fresh file every time
	os.Remove(path)
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		log.Fatal(err)
	}

	// create bucket
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucket)
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	return db
}

func hellobolt() {
	db := prepBolt(1)
	defer db.Close()

	err := db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		err := b.Put([]byte("answer"), []byte("42"))
		return err
	})
	if err != nil {
		log.Fatal(err)
	}

	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		v := b.Get([]byte("answer"))
		fmt.Printf("value: %s\n", v)
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
}

func keyValue(i int) (key string, value []string) {
	key = strconv.Itoa(i)
	value = make([]string, 5)
	for i := range value {
		value[i] = strings.Repeat(key, i)
	}
	return key, value
}

func writeTest(myDb db, size int) (duration time.Duration) {
	start := time.Now()
	var key string
	var value []string
	for i := 0; i < size; i++ {
		key, value = keyValue(i)
		myDb.Writer(key, value)
	}
	myDb.Flush()
	return time.Since(start)
}

func main() {
	hellobolt()

	size := 1000000
	fmt.Printf("number of entries: %d\n", size)

	mapDb := newMapType()
	mapTime := writeTest(mapDb, size)
	fmt.Printf("Write map test took: %s\n", mapTime)

	mapBolt := newBoltType(size / 5)
	defer mapBolt.Db.Close()
	boltTime := writeTest(mapBolt, size)
	fmt.Printf("Write bolt test took: %s\n", boltTime)

	fmt.Printf("Write bolt/map: %1.1fX\n",
		float64(boltTime.Nanoseconds())/float64(mapTime.Nanoseconds()))

	// sanity check, read everything
	start := time.Now()
	mapBolt.Db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		var storedValue []string
		for i := 0; i < size; i++ {
			key := strconv.Itoa(i)
			err := json.Unmarshal(b.Get([]byte(key)), &storedValue)
			if err != nil {
				log.Fatal(err)
			}
			if i == 1 {
				fmt.Println("stored value:", storedValue)
			}
		}
		return nil
	})
	fmt.Printf("Read bolt test took: %s\n", time.Since(start))

}
