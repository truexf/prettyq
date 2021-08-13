// Copyright 2021 fangyousong(方友松). All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package memoryq

import (
	"fmt"
	"sync/atomic"

	"github.com/truexf/goutil"
	"github.com/truexf/prettyq/protocol"
)

// 内存中的消息队列，保存了各topic的最新的消息，超过队列长度的消息自动剔除
type MemoryQ struct {
	cap  int64
	data *goutil.LinkedList
}

func NewMemoryQ(cap int) (*MemoryQ, error) {
	if cap <= 0 {
		return nil, fmt.Errorf("q.cap: %d is invalid", cap)
	}
	return &MemoryQ{cap: int64(cap), data: goutil.NewLinkedList(true)}, nil
}

func (m *MemoryQ) PubMsg(msg *protocol.MessageStorage) error {
	if m.cap >= int64(m.data.Len) {
		m.data.PopHead(true)
	}

	m.data.PushTail(msg, true)
	atomic.AddInt64(&m.cap, 1)
	return nil
}

func (m *MemoryQ) ConsumeMsg() (*protocol.MessageConsumer, error) {
	msg := m.data.PopHead(true)
	if msg == nil {
		return nil, protocol.NewError(protocol.ErrorCodeNoMessage, protocol.ErrorMsgNoMessage)
	}
	defer atomic.AddInt64(&m.cap, -1)
	return protocol.MessageStorageToMessageConsumer(msg.(*protocol.MessageStorage)), nil
}
