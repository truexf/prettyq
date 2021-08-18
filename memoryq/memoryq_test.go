package memoryq

import (
	"fmt"
	"testing"

	"github.com/truexf/prettyq/protocol"
)

func TestMemoryq(t *testing.T) {
	q, err := NewMemoryQ(10000)
	if err != nil {
		t.Fatalf(err.Error())
	}
	for i := 1; i <= 12345; i++ {
		if err := q.PubMsg(protocol.NewMessageStorage([]byte(fmt.Sprintf("test-msg-%d", i)), uint64(i))); err != nil {
			t.Fatalf(err.Error())
		}
	}

	first := true
	var lastNum uint64 = 0
	for {
		if msg, err := q.ConsumeMsg(); err != nil {
			if _, ok := err.(*protocol.Error); !ok {
				t.Fatalf(err.Error())
			}
			fmt.Printf("last msg num: %d\n", lastNum)
			break
		} else {
			lastNum = msg.Num
			if first {
				if msg.Num != 2346 {
					t.Fatalf("fail")
				}
				fmt.Printf("first msg num: %d\n", msg.Num)
				first = false
			}

		}
	}
}
