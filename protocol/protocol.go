// Copyright 2021 fangyousong(方友松). All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package protocol

type MessageStorage struct {
	Num  uint64
	Size uint32
	Data []byte
	Crc  uint32
}

type MessagePublisher struct {
	Num       uint64
	Size      uint32
	TopicSize uint16
	Topic     []byte
	Data      []byte
	Crc       uint32
}

type MessageConsumer struct {
	Num  uint64
	Size uint32
	Data []byte
	Crc  uint32
}
