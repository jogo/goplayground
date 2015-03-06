/*
Testing out boltdb for the following use case:

* Load several million key/value pairs in as quickly as possible
* Data forms a graph, that will be searched using A*
* Load once, search many times
* Data is too big to be all be in memory

Issues so far:

* Writing to boltdb involves 2 writes to disk, so performance is terrible if writes aren't batched

Ideas to try out:

* Built test suite with a regular map
* Swap in boltdb backend and compare.
* Try out boltdb transaction coalescer
  https://github.com/boltdb/coalescer
* Separate test to measure how long it takes to read all the values back.


Findings:

* Overhead of db.Update for single key/value write is massive.
  At 1 million keys per db.Update overhead  still 5x slower

Next try coalescer

*/

package main

import (
	"fmt"
	"github.com/boltdb/bolt"
	"log"
	"strconv"
	"strings"
	"time"
)

var bucket = []byte("MyBucket")

func prepBolt() *bolt.DB {
	db, err := bolt.Open("my.db", 0600, nil)
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
	db := prepBolt()
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

func writeMapTest(size int) {
	db := make(map[(string)]string)
	var key string
	var value string
	for i := 0; i < size; i++ {
		key, value = keyValue(i)
		//fmt.Printf("%s: %s\n", key, value)
		db[key] = value
	}

}

func writeBoltTest(size int) {
	db := prepBolt()
	defer db.Close()

	var key string
	var value string
	err := db.Update(func(tx *bolt.Tx) error {
		for i := 0; i < size; i++ {
			key, value = keyValue(i)
			b := tx.Bucket(bucket)
			err := b.Put([]byte(key), []byte(value))
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

}

func main() {
	hellobolt()
	size := 1000000
	start := time.Now()
	writeMapTest(size)
	mapTime := time.Since(start)
	fmt.Printf("Map Test took: %s\n", mapTime)
	start = time.Now()
	writeBoltTest(size)
	boltTime := time.Since(start)
	fmt.Printf("Bolt Test took: %s\n", boltTime)
	fmt.Printf("bolt/map: %1.1fX\n",
		float64(boltTime.Nanoseconds())/float64(mapTime.Nanoseconds()))
}
