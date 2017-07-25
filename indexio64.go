package indexio

import (
	"encoding/binary"

	"github.com/Path94/turtleDB"
)

// New64 will return a new instance of 64-bit indexio
func New64(path string) (ip *Indexio64, err error) {
	var i Indexio64
	// Initialize functions map for marshaling and unmarshaling
	fm := turtleDB.NewFuncsMap(marshal64, unmarshal64)
	// Create new instance of turtleDB
	if i.db, err = turtleDB.New("indexio", path, fm); err != nil {
		return
	}
	// Initialize indexes bucket
	if err = i.db.Update(initBucket); err != nil {
		return
	}
	// Assign ip as a pointer to i
	ip = &i
	return
}

// Indexio64 manages indexes
type Indexio64 struct {
	db *turtleDB.Turtle
}

// Next will return the next index for a given key
func (i *Indexio64) Next(key string) (idx uint64, err error) {
	err = i.db.Update(func(txn turtleDB.Txn) (err error) {
		var bkt turtleDB.Bucket
		if bkt, err = txn.Get("indexes"); err != nil {
			return
		}

		if idx, err = getCurrent64(bkt, key); err != nil {
			return
		}

		// Set the index as the next value
		return bkt.Put(key, idx+1)
	})

	return
}

// Current will return the current index for a given key
func (i *Indexio64) Current(key string) (idx uint64, err error) {
	err = i.db.Read(func(txn turtleDB.Txn) (err error) {
		var bkt turtleDB.Bucket
		if bkt, err = txn.Get("indexes"); err != nil {
			return
		}

		idx, err = getCurrent64(bkt, key)
		return
	})

	if idx == 0 {
		err = ErrKeyUnset
		return
	}

	idx--
	return
}

// Close will close an instance of indexio
func (i *Indexio64) Close() error {
	return i.db.Close()
}

func marshal64(val turtleDB.Value) (b []byte, err error) {
	var (
		idx uint64
		ok  bool
	)

	if idx, ok = val.(uint64); !ok {
		return
	}

	b = make([]byte, 8)
	binary.LittleEndian.PutUint64(b, idx)
	return
}

func unmarshal64(b []byte) (val turtleDB.Value, err error) {
	idx := binary.LittleEndian.Uint64(b)
	val = idx
	return
}

func getCurrent64(bkt turtleDB.Bucket, key string) (idx uint64, err error) {
	var (
		val turtleDB.Value
		ok  bool
	)

	if val, err = bkt.Get(key); err != nil {
		err = nil
		return
	}

	// The value exists, let's assert the value as a uint64 and set idx
	if idx, ok = val.(uint64); !ok {
		err = ErrInvalidType
		return
	}

	return
}
