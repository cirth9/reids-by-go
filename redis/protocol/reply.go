package protocol

import (
	"bytes"
	"reids-by-go/interface/redis"
	"strconv"
	"time"
)

type StatusReply struct {
	Status string
}

func MakeStatusReply(content string) redis.Reply {
	return &StatusReply{
		Status: content,
	}
}

func (s *StatusReply) ToBytes() []byte {
	return []byte("+" + s.Status + CRLF)
}

type IntReply struct {
	value int64
}

func MakeIntReply(value int64) redis.Reply {
	return &IntReply{value: value}
}

func (i *IntReply) ToBytes() []byte {
	return []byte(":" + strconv.FormatInt(i.value, 10) + CRLF)
}

type FloatReply struct {
	value float64
}

func MakeFloatReply(value float64) redis.Reply {
	return &FloatReply{value: value}
}

func (f *FloatReply) ToBytes() []byte {
	return []byte(":" + strconv.FormatFloat(f.value, 'g', -1, 64) + CRLF)
}

type BulkReply struct {
	Arg []byte
}

func MakeBulkReply(arg []byte) redis.Reply {
	return &BulkReply{
		Arg: arg,
	}
}

func MakeNullBulkReply() redis.Reply {
	return &BulkReply{}
}

func (b *BulkReply) ToBytes() []byte {
	if b.Arg == nil {
		return []byte(NullBulkBytes)
	}
	return []byte("$" + strconv.Itoa(len(b.Arg)) + CRLF + string(b.Arg) + CRLF)
}

type MultiBulkStringReply struct {
	Args [][]byte
}

func MakeNullMultiBulk() redis.Reply {
	return &MultiBulkStringReply{}
}

func MakeMultiBulkReply(args [][]byte) redis.Reply {
	return &MultiBulkStringReply{
		Args: args,
	}
}

func (m *MultiBulkStringReply) ToBytes() []byte {
	argLen := len(m.Args)
	var buf bytes.Buffer
	buf.WriteString("*" + strconv.Itoa(argLen) + CRLF)
	for _, arg := range m.Args {
		if arg == nil {
			buf.WriteString("$-1" + CRLF)
		} else {
			buf.WriteString("$" + strconv.Itoa(len(arg)) + CRLF + string(arg) + CRLF)
		}
	}
	return buf.Bytes()
}

type TimeReply struct {
	Time time.Time
}

func MakeTimeReply(time time.Time) redis.Reply {
	return &TimeReply{Time: time}
}

func (t *TimeReply) ToBytes() []byte {
	return []byte("+" + t.Time.String() + CRLF)
}
