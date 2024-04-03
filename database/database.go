package database

import (
	"reids-by-go/datastruct/dict"
)

const (
	dataDictSize = 1 << 16
	ttlDictSize  = 1 << 10
)

type DB struct {
	index int
	// key -> DataEntity
	data *dict.ConcurrentDict

	// key -> expireTime (time.Time)
	ttlMap *dict.ConcurrentDict

	// key -> version(uint32)
	versionMap *dict.ConcurrentDict

	//// callbacks
	//insertCallback database.KeyEventCallback
	//deleteCallback database.KeyEventCallback

	//todo   about aof

	// addaof is used to add command to aof
	addAof func(CmdLine)
}

type CmdLine = [][]byte

func NewDatabase() DB {
	return DB{
		index:      0,
		data:       dict.MakeConcurrent(dataDictSize),
		ttlMap:     dict.MakeConcurrent(ttlDictSize),
		versionMap: dict.MakeConcurrent(dataDictSize),
		addAof:     func(line CmdLine) {},
	}
}
