// Copyright 2021 fangyousong(方友松). All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package protocol

import (
	"hash/crc32"
	"time"
)

type Error struct {
	Code    int
	Message string
	Tm      time.Time
}

func (m *Error) Error() string {
	return m.Message
}

func NewError(code int, msg string) *Error {
	return &Error{Code: code, Message: msg, Tm: time.Now()}
}

// 数据文件单条记录，数据文件由n条MessageStorage组成，其中整数字段均为BigEndian
type MessageStorage struct {
	Num      uint64
	Size     uint32 //等于Data和crc的长度之和
	Data     []byte
	Crc      uint32
	TailSize uint32 //等于size, 放在每一条消息记录的末尾，方便对消息文件进行从尾部开始的回溯
}

func NewMessageStorage(data []byte, messageNum uint64) *MessageStorage {
	ret := &MessageStorage{Data: data, Num: messageNum}
	ret.Size = uint32(len(data) + 4)
	ret.TailSize = ret.Size
	ret.Crc = crc32.ChecksumIEEE(data)
	return ret
}

// 发布的消息格式
type MessagePublisher struct {
	Num       uint64
	Size      uint32 //等于后续字段长度之和
	TopicSize uint16
	Topic     []byte
	Data      []byte
	Crc       uint32
}

func MessagePublisherToMessageStorage(msg *MessagePublisher) *MessageStorage {
	if msg == nil {
		return nil
	}
	ret := &MessageStorage{Num: msg.Num, Data: msg.Data, Crc: msg.Crc}
	ret.Size = uint32(len(msg.Data) + 4)
	ret.TailSize = ret.Size
	return ret
}

func MessageStorageToMessageConsumer(msg *MessageStorage) *MessageConsumer {
	if msg == nil {
		return nil
	}
	return &MessageConsumer{Num: msg.Num, Size: msg.Size, Data: msg.Data, Crc: msg.Crc}
}

// 消费的消息格式
type MessageConsumer struct {
	Num  uint64
	Size uint32 //等于Data和Crc的长度之和
	Data []byte
	Crc  uint32
}

type MessageQueryResult struct {
	Message *MessageConsumer
	Error   error
}

// 对存储引擎的查询请求抽象
type MessageQuery struct {
	Topic      string
	MessagePos uint64
	Limit      int
	Result     chan *MessageQueryResult //查询结果
}
