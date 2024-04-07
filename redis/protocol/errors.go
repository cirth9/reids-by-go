package protocol

import (
	"errors"
	"reids-by-go/interface/redis"
)

type ErrReplyInterface interface {
	Error() error
	ToBytes() []byte
}

type ErrReply struct {
	err string
}

func MakeErrReply(s string) redis.Reply {
	return &ErrReply{err: s}
}

func (e *ErrReply) Error() error {
	return errors.New(e.err)
}

func (e *ErrReply) ToBytes() []byte {
	return []byte("-" + e.err + CRLF)
}
