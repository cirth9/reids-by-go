package timewheel

import (
	"log"
	"testing"
	"time"
)

func TestDelay(t *testing.T) {
	ch := make(chan time.Time)
	beginTime := time.Now()
	log.Println("now")
	Delay(time.Second, "", func() {
		log.Println("delay")
		ch <- time.Now()
	})
	execAt := <-ch
	delayDuration := execAt.Sub(beginTime)
	// usually 1.0~2.0 s
	if delayDuration < time.Second || delayDuration > 3*time.Second {
		t.Error("wrong execute time")
	}
	t.Log(delayDuration)
}
