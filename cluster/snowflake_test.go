package cluster

import (
	"log"
	"testing"
)

func TestSnow(t *testing.T) {
	log.Println(hashFnv32("test"))
	worker, err := NewWorker(hashFnv32("test1"))
	if err != nil {
		t.Error(err)
	}
	println(worker.GetId())
}
