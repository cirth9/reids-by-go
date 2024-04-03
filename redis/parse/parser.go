package parse

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"log"
	"reids-by-go/interface/redis"
	"reids-by-go/redis/protocol"
	"strconv"
)

/*
	RESP(REdis Serialization Protocol) 通过第一个字符来表示格式：
	简单字符串：以"+" 开始， 如："+OK\r\n"
	错误：以"-" 开始，如："-ERR Invalid Synatx\r\n"
	整数：以":"开始，如：":1\r\n"
	字符串：以 $ 开始
	数组：以 * 开始

	来自客户端的请求均为数组格式，它在第一行中标记报文的总行数并使用CRLF作为分行符。
*/

var suffixCRLF = []byte{'\r', '\n'}

type Payload struct {
	Data redis.Reply
	Err  error
}

// ParseStream  流式处理，通过io.reader读取数据，然后通过channel将结果返回给调用者
func ParseStream(reader io.Reader) <-chan *Payload {
	ch := make(chan *Payload)
	go parse0(reader, ch)
	return ch
}

// ParseOne 单点处理，解析[]byte，然后返回redis.reply
func ParseOne(data []byte) (redis.Reply, error) {
	ch := make(chan *Payload)
	reader := bytes.NewReader(data)
	go parse0(reader, ch)
	payload := <-ch
	if payload == nil {
		return nil, errors.New("no reply")
	}
	return payload.Data, payload.Err
}

// todo parse0为解析器核心流程，解析完毕后发往ch
func parse0(rawReader io.Reader, ch chan *Payload) {
	defer func() {
		if r := recover(); r != nil {
			log.Println(r)
		}
	}()

	reader := bufio.NewReader(rawReader)
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			ch <- &Payload{Err: err}
			close(ch)
			return
		}
		length := len(line)
		if length <= 2 || line[length-2] != '\r' {
			continue
		}
		//todo 全部为\r\n结尾，先去除尾部的crlf
		line = bytes.TrimSuffix(line, suffixCRLF)
		//todo line byte数组首字母用于标识
		//log.Println("parse line ", string(line))
		switch line[0] {
		case '+':
			//todo 简单字符串，例如: +OK\r\n
			content := string(line[1:])
			ch <- &Payload{
				Data: protocol.MakeStatusReply(content),
			}
			//if strings.HasPrefix(content, "FULLRESYNC") {
			//	err = parseRdbBulkString(reader, ch)
			//	if err != nil {
			//		ch <- &Payload{err: err}
			//		close(ch)
			//		return
			//	}
			//}
		case '-':
			//todo 错误，例如:	-err\r\n
			ch <- &Payload{
				Data: protocol.MakeErrReply(string(line[1:])),
			}
		case ':':
			//todo 整数，例如： :1\r\n
			value, err := strconv.ParseInt(string(line[1:]), 10, 64)
			if err != nil {
				ch <- &Payload{Err: err}
				close(ch)
				return
			}
			ch <- &Payload{
				Data: protocol.MakeIntReply(value),
			}
		case '$':
			//todo 字符串，$后一个数字代表正文长度,例如： $3\r\nSET\r\n 	$-1(nil)
			err = parseBulkString(line, reader, ch)
			if err != nil {
				ch <- &Payload{Err: err}
				close(ch)
				return
			}
		case '*':
			//todo 数组，*后一个数字代表数组长度,例如：*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\value\r\n (["SET","key","value"])
			err = parseArray(line, reader, ch)
			if err != nil {
				ch <- &Payload{Err: err}
				close(ch)
				return
			}
		default:
			args := bytes.Split(line, []byte{' '})
			ch <- &Payload{
				Data: protocol.MakeMultiBulkReply(args),
			}
		}
	}
}

func parseArray(header []byte, reader *bufio.Reader, ch chan *Payload) (err error) {
	//todo *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\value\r\n (["SET","key","value"])
	arrLen, err := strconv.ParseInt(string(bytes.TrimSuffix(header, suffixCRLF)[1:]), 10, 32)
	if err != nil || arrLen < 0 {
		protocolError(ch, "parse arr len error")
		//log.Println(arrLen, err)
		return nil
	} else if arrLen == 0 {
		ch <- &Payload{
			Data: protocol.MakeNullMultiBulk(),
		}
		return nil
	}
	lines := make([][]byte, 0, arrLen)
	for i := 0; i < int(arrLen); i++ {
		var line []byte
		strHeader, err := reader.ReadBytes('\n')
		//log.Println(string(strHeader))
		if err != nil {
			return err
		}
		strLen, err := strconv.ParseInt(string(bytes.TrimSuffix(strHeader, suffixCRLF)[1:]), 10, 64)
		if err != nil || strLen < -1 {
			protocolError(ch, "illegal bulk string header")
			return nil
		} else if strLen == -1 {
			lines = append(lines, []byte{})
			return nil
		}

		line = make([]byte, strLen+2)
		_, err = io.ReadFull(reader, line)
		//log.Println(string(line))
		if err != nil {
			return err
		}
		lines = append(lines, line)
	}
	ch <- &Payload{
		Data: protocol.MakeMultiBulkReply(lines),
	}
	return
}

func parseBulkString(header []byte, reader *bufio.Reader, ch chan *Payload) (err error) {
	strLen, err := strconv.ParseInt(string(bytes.TrimSuffix(header, suffixCRLF)[1:]), 10, 64)
	//log.Println("parse Bulk String Len: ", strLen)
	if err != nil || strLen < -1 {
		protocolError(ch, "illegal bulk string header: "+string(header))
		return nil
	} else if strLen == -1 {
		ch <- &Payload{
			Data: protocol.MakeNullBulkReply(),
		}
		return nil
	}
	body := make([]byte, strLen+2)
	_, err = io.ReadFull(reader, body)
	//log.Println("parse body ,", string(body))
	//log.Println("parse bulk string ,", string(protocol.MakeBulkReply(body[:len(body)-2]).ToBytes()))
	if err != nil {
		return err
	}
	ch <- &Payload{
		Data: protocol.MakeBulkReply(body[:len(body)-2]),
	}
	return nil
}

func parseRdbBulkString(reader *bufio.Reader, ch chan *Payload) (err error) {
	//header, err := reader.ReadBytes('\n')
	//if err != nil {
	//	return err
	//}
	//header :=
	return
}

func protocolError(ch chan<- *Payload, msg string) {
	err := errors.New("protocol error: " + msg)
	ch <- &Payload{Err: err}
}
