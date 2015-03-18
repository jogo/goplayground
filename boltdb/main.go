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
* Separate test to measure how long it takes to read all the values back.


Findings:

* Overhead of db.Update for single key/value write is massive.
  At 1 million keys per db.Update overhead  still 5x slower

coalescer -- Not working well even on an SSD, but works. Go back to home built solution.
 (Found issue with coalescer logic)



*/

package main

import (
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
	Writer(key, value string)
	Flush()
}

type mapType struct {
	db map[string]string
}

func (m *mapType) Writer(key, value string) {
	m.db[key] = value
}

func (m *mapType) Flush() {
}

func NewMapType() *mapType {
	m := mapType{
		db: make(map[string]string),
	}
	return &m
}

type boltType struct {
	Db        *bolt.DB
	buffer    map[string]string
	batchSize int
}

func NewBoltType(limit int) *boltType {
	db := prepBolt(limit)
	b := boltType{
		Db:     db,
		buffer: make(map[string]string),
		// If batch is too things slow down
		batchSize: 10000,
	}
	return &b
}

func (mybolt *boltType) Writer(key, value string) {
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
			err := b.Put([]byte(key), []byte(value))
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

func keyValue(i int) (key, value string) {
	key = strconv.Itoa(i)
	value = strings.Repeat(key, 5)
	return key, value
}

func writeTest(myDb db, size int) (duration time.Duration) {
	start := time.Now()
	var key string
	var value string
	for i := 0; i < size; i++ {
		key, value = keyValue(i)
		myDb.Writer(key, value)
	}
	myDb.Flush()
	return time.Since(start)
}

func main() {
	hellobolt()

	size := 500000
	fmt.Printf("number of entries: %d\n", size)

	mapDb := NewMapType()
	mapTime := writeTest(mapDb, size)
	fmt.Printf("Map Test took: %s\n", mapTime)

	mapBolt := NewBoltType(size / 5)
	defer mapBolt.Db.Close()

	boltTime := writeTest(mapBolt, size)
	fmt.Printf("Bolt Test took: %s\n", boltTime)

	// sanity check
	mapBolt.Db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		key, value := keyValue(size - 1)
		storedValue := b.Get([]byte(key))
		if value != string(storedValue) {
			fmt.Printf("something went wrong, with the stored value: %s\n", storedValue)
		}
		return nil
	})

	fmt.Printf("bolt/map: %1.1fX\n",
		float64(boltTime.Nanoseconds())/float64(mapTime.Nanoseconds()))

}
