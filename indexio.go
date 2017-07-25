package indexio

import (
	"encoding/binary"

	"github.com/Path94/turtleDB"
	"github.com/missionMeteora/toolkit/errors"
)

const (
	// ErrInvalidType is returned when an invalid type is encountered
	ErrInvalidType = errors.Error("invalid type")
	// ErrKeyUnset is returned when a key is queried and does not exist
	ErrKeyUnset = errors.Error("key unset")
)

// New will return a new instance of indexio
func New(path string) (ip *Indexio, err error) {
	var i Indexio
	// Initialize functions map for marshaling and unmarshaling
	fm := turtleDB.NewFuncsMap(marshal, unmarshal)
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

// Indexio manages indexes
type Indexio struct {
	db *turtleDB.Turtle
}

func (i *Indexio) initBucket(txn turtleDB.Txn) (err error) {
	_, err = txn.Create("indexes")
	return
}

// Next will return the next index for a given key
func (i *Indexio) Next(key string) (idx uint32, err error) {
	err = i.db.Update(func(txn turtleDB.Txn) (err error) {
		var bkt turtleDB.Bucket
		if bkt, err = txn.Get("indexes"); err != nil {
			return
		}

		if idx, err = getCurrent(bkt, key); err != nil {
			return
		}

		// Set the index as the next value
		return bkt.Put(key, idx+1)
	})

	return
}

// Current will return the current index for a given key
func (i *Indexio) Current(key string) (idx uint32, err error) {
	err = i.db.Read(func(txn turtleDB.Txn) (err error) {
		var bkt turtleDB.Bucket
		if bkt, err = txn.Get("indexes"); err != nil {
			return
		}

		idx, err = getCurrent(bkt, key)
		return
	})

	if idx == 0 {
		err = ErrKeyUnset
		return
	}

	idx--
	return
}

func (i *Indexio) Close() error {
	return i.db.Close()
}

func marshal(val turtleDB.Value) (b []byte, err error) {
	var (
		idx uint32
		ok  bool
	)

	if idx, ok = val.(uint32); !ok {
		return
	}

	b = make([]byte, 8)
	binary.LittleEndian.PutUint32(b, idx)
	return
}

func unmarshal(b []byte) (val turtleDB.Value, err error) {
	idx := binary.LittleEndian.Uint32(b)
	val = idx
	return
}

func initBucket(txn turtleDB.Txn) (err error) {
	_, err = txn.Create("indexes")
	return
}

func getCurrent(bkt turtleDB.Bucket, key string) (idx uint32, err error) {
	var (
		val turtleDB.Value
		ok  bool
	)

	if val, err = bkt.Get(key); err != nil {
		err = nil
		return
	}

	// The value exists, let's assert the value as a uint32 and set idx
	if idx, ok = val.(uint32); !ok {
		err = ErrInvalidType
		return
	}

	return
}
