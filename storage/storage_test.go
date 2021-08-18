package storage

import (
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/truexf/prettyq/protocol"
)

func TestMessageFile(t *testing.T) {
	os.RemoveAll("/tmp/prettyq/storage")
	os.MkdirAll("/tmp/prettyq/storage", 0777)
	// write message
	msgFile, err := NewMessageFile("/tmp/prettyq/storage", "test-topic", 0, 1024*1024)
	if err != nil {
		t.Fatalf(err.Error())
	}
	for i := 0; i < 1234; i++ {
		msg := strings.Repeat("testmessage", 1000) + strconv.Itoa(i)
		msgFile.WriteMessage(protocol.NewMessageStorage([]byte(msg), uint64(i)))
	}
	msgFile.Close()

	// read message
	pool := NewIndexPool()
	pool.RegisterTopicDataPath("test-topic", "/tmp/prettyq/storage")
	for i := 1233; i >= 0; i-- {
		q := &protocol.MessageQuery{Topic: "test-topic",
			MessagePos: uint64(i),
			Limit:      1,
			Result:     make(chan *protocol.MessageQueryResult, 2)}
		pool.Query(q)
		ret := <-q.Result
		if ret.Error != nil {
			t.Fatalf(ret.Error.Error())
		}
		if string(ret.Message.Data) != strings.Repeat("testmessage", 1000)+strconv.Itoa(i) {
			t.Fatalf("not equal, %s, %d", string(ret.Message.Data), i)
		}
		// fmt.Printf("message pos: %d, ok\n", i)
	}
}

func BenchmarkQuery(t *testing.B) {
	c := make(chan *protocol.MessageQueryResult, 2)
	pool := NewIndexPool()
	pool.RegisterTopicDataPath("test-topic", "/tmp/prettyq/storage")
	for i := 0; i < t.N; i++ {
		n := rand.Intn(1234)
		q := &protocol.MessageQuery{Topic: "test-topic",
			MessagePos: uint64(n),
			Limit:      1,
			Result:     c}
		pool.Query(q)
		ret := <-q.Result
		if ret.Error != nil {
			t.Fatalf(ret.Error.Error())
		}
		if string(ret.Message.Data) != strings.Repeat("testmessage", 1000)+strconv.Itoa(n) {
			t.Fatalf("not equal, %s, %d", string(ret.Message.Data), n)
		}
	}
}
