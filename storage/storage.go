// Copyright 2021 fangyousong(方友松). All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package storage

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/truexf/prettyq/protocol"
)

// MessageFile代表一个topic的数据文件，storage engine通过其实现对数据文件的读写
type MessageFile struct {
	filePath            string     //文件所在目录
	topic               string     //topic
	messagePos          uint32     //文件中第一条消息的消息序号
	fileMaxSize         uint32     //文件最大尺寸
	fd                  *os.File   //文件读写句柄
	currentFileSize     int64      //当前文件实际尺寸
	currentfullFileName string     //全路径文件名
	idxFile             *IndexFile //对应的索引文件
}

// IndexFile代表一个MessageFile对应的索引文件，索引文件与数据文件是一一对应的
type IndexFile struct {
	filePath                string   //文件所在目录
	topic                   string   //topic
	currentfullFileName     string   //全路径文件名
	messagePos              uint32   //文件中第一条索引的消息序号（索引的key）
	latestIndexedMessagePos uint32   //文件中最后一条索引的消息序号
	fd                      *os.File //文件读写句柄
}

func newIndexFile(filePath string, topic string) (*IndexFile, error) {
	ret := &IndexFile{filePath: filePath, topic: topic}
	ret.currentfullFileName = ret.nextFileName()
	var err error
	ret.fd, err = os.OpenFile(ret.currentfullFileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, fmt.Errorf("open file %s fail, %s", ret.currentfullFileName, err.Error())
	}
	return ret, nil
}

func (m *IndexFile) nextFileName() string {
	fn := fmt.Sprintf("%s_%d.idx", m.topic, m.messagePos)
	return filepath.Join(m.filePath, fn)
}

func (m *IndexFile) writeIndex(messagePos uint32, filePos int64) error {
	m.messagePos = messagePos
	if m.messagePos-m.latestIndexedMessagePos >= 10 {
		rec := fmt.Sprintf("%d:%d\n", messagePos, filePos)
		m.fd.WriteString(rec)
	}
	return nil
}

func (m *IndexFile) Close() {
	m.fd.Close()
}

func NewMessageFile(filePath string, topic string, startMessagePos uint32, maxSize uint32) (*MessageFile, error) {
	ret := &MessageFile{filePath: filePath, topic: topic, messagePos: startMessagePos, fileMaxSize: maxSize}
	ret.currentfullFileName = ret.nextFileName()
	var err error
	ret.fd, err = os.OpenFile(ret.currentfullFileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, fmt.Errorf("open file %s fail, %s", ret.currentfullFileName, err.Error())
	}
	if ret.idxFile, err = newIndexFile(filePath, topic); err != nil {
		return nil, err
	}

	return ret, nil
}

func (m *MessageFile) nextFileName() string {
	fn := fmt.Sprintf("%s_%d.data", m.topic, m.messagePos)
	return filepath.Join(m.filePath, fn)
}

func (m *MessageFile) WriteMessage(msg *protocol.MessageStorage) error {
	bts := make([]byte, 16+msg.Size)
	binary.BigEndian.PutUint64(bts, msg.Num)
	binary.BigEndian.PutUint32(bts[8:], msg.Size)
	copy(bts[12:], msg.Data)
	binary.BigEndian.PutUint32(bts[12+len(msg.Data):], msg.Crc)
	binary.BigEndian.PutUint32(bts[12+len(msg.Data)+4:], msg.Size)
	if _, err := m.fd.Write(bts); err != nil {
		return err
	}
	m.currentFileSize += int64(len(bts))
	m.messagePos++

	// write index
	if err := m.idxFile.writeIndex(m.messagePos, m.currentFileSize); err != nil {
		return err
	}

	// create new file if file size exceed
	if m.currentFileSize >= int64(m.fileMaxSize) {
		m.fd.Close()
		m.idxFile.Close()
		m.currentfullFileName = m.nextFileName()
		var err error
		m.fd, err = os.OpenFile(m.currentfullFileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			return fmt.Errorf("open file %s fail, %s", m.currentfullFileName, err.Error())
		}
		if m.idxFile, err = newIndexFile(m.filePath, m.topic); err != nil {
			return err
		}
	}
	return nil
}

func (m *MessageFile) Close() {
	m.fd.Close()
	m.idxFile.Close()
}

type MessageIndexRec struct {
	MessagePos uint64
	FilePos    int64
}

// MessageIndex是IndexFile在内存中的结构化对象。
// 索引文件为稀疏索引，每10条消息一条索引，按\n分隔， 一个索引文件对应一个数据文件。一个索引文件一次性加载于内存，
// 其按MessagePos排序，方便查找时进行折半高效查找
type MessageIndex struct {
	topic           string
	indexFile       string            //索引文件名
	startMessagePos uint64            //起始索引的消息序号
	stopMessagePos  uint64            //截至索引的消息序号（当前索引文件并不含该消息序号的索引, 该消息序号是下一个索引文件的起始消息序号）
	data            []MessageIndexRec //排序索引的记录
	sync.RWMutex
}

func (a *MessageIndex) Len() int           { return len(a.data) }
func (a *MessageIndex) Swap(i, j int)      { a.data[i], a.data[j] = a.data[j], a.data[i] }
func (a *MessageIndex) Less(i, j int) bool { return a.data[i].MessagePos < a.data[j].MessagePos }

func NewMessageIndex(topic string) *MessageIndex {
	return &MessageIndex{topic: topic, stopMessagePos: math.MaxUint64, data: make([]MessageIndexRec, 0, 1024)}
}
func newMessageIndexFromFile(idxFile string) (*MessageIndex, error) {
	if !strings.HasSuffix(idxFile, ".idx") {
		return nil, fmt.Errorf("file name %s not has suffix .idx", idxFile)
	}
	fn := strings.TrimSuffix(idxFile, ".idx")
	fnParts := strings.Split(fn, "_")
	if len(fnParts) != 2 {
		return nil, fmt.Errorf("invalid index file name %s", idxFile)
	}
	if _, err := strconv.ParseUint(fnParts[1], 10, 16); err != nil {
		return nil, fmt.Errorf("invalid index file name %s", idxFile)
	}

	bts, err := ioutil.ReadFile(idxFile)
	if err != nil {
		return nil, err
	}
	lines := bytes.Split(bts, []byte("\n"))
	ret := &MessageIndex{topic: fnParts[0], data: make([]MessageIndexRec, 0, len(lines))}
	for _, v := range lines {
		if len(v) == 0 {
			continue
		}
		parts := strings.Split(string(v), ":")
		if len(parts) != 2 {
			continue
		}
		msgPos, err := strconv.ParseUint(parts[0], 10, 64)
		if err != nil {
			continue
		}
		filePos, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			continue
		}
		ret.data = append(ret.data, MessageIndexRec{MessagePos: msgPos, FilePos: filePos})
	}
	sort.Sort(ret)
	return ret, nil
}

func (m *MessageIndex) AppendIndexRec(rec MessageIndexRec) error {
	m.Lock()
	defer m.Unlock()
	if len(m.data) > 0 && m.data[len(m.data)-1].MessagePos >= rec.MessagePos {
		return fmt.Errorf("mesasge index is too old")
	}
	m.data = append(m.data, rec)
	if m.startMessagePos == 0 {
		m.startMessagePos = rec.MessagePos
		m.indexFile = fmt.Sprintf("%s_%d.idx", m.topic, rec.MessagePos)
	}
	return nil
}

func (m *MessageIndex) Find(messagePos uint64) *MessageIndexRec {
	m.RLock()
	defer m.RUnlock()

	idx := sort.Search(len(m.data), func(i int) bool {
		if len(m.data) == 0 || messagePos >= m.stopMessagePos {
			return false
		}
		return (len(m.data) == i+1 && m.data[i].MessagePos >= messagePos) ||
			(len(m.data) > i+1 && m.data[i].MessagePos <= messagePos && m.data[i+1].MessagePos >= messagePos)
	})
	if idx == len(m.data) {
		return nil
	}
	return &m.data[idx]
}

// 一个topic的索引文件集，data式一个顺序排列的索引文件cache
// currentIndexFile指向当前索引文件，storage写入索引文件的同时，会向当前cache中写入索引数据，避免从文件加载当前未完成的索引文件
type MessageIndexes struct {
	topic            string
	data             []*MessageIndex
	currentIndexFile string
	sync.RWMutex
}

func (a *MessageIndexes) Len() int      { return len(a.data) }
func (a *MessageIndexes) Swap(i, j int) { a.data[i], a.data[j] = a.data[j], a.data[i] }
func (a *MessageIndexes) Less(i, j int) bool {
	return a.data[i].startMessagePos < a.data[j].startMessagePos
}

func newMessageIndexes(topic string) *MessageIndexes {
	return &MessageIndexes{topic: topic, data: make([]*MessageIndex, 0, 10)}
}

func (m *MessageIndexes) AddIndex(idx *MessageIndex) {
	m.Lock()
	defer m.RLock()
	i := sort.Search(len(m.data), func(i int) bool {
		if i == len(m.data)-1 {
			return m.data[i].indexFile == idx.indexFile
		}
		return m.data[i].indexFile <= idx.indexFile && m.data[i+1].indexFile >= idx.indexFile
	})
	if i == len(m.data) {
		m.data = append(m.data, idx)
	} else {
		ret := m.data[:i]
		ret = append(ret, idx)
		ret = append(ret, m.data[i:]...)
		m.data = ret
	}
}

func (m *MessageIndexes) AppendIndexRec(rec MessageIndexRec) error {
	idx := m.Find(rec.MessagePos)
	if idx == nil {
		idx = NewMessageIndex(m.topic)
	}
	if err := idx.AppendIndexRec(rec); err != nil {
		return err
	}
	m.Lock()
	defer m.Unlock()
	m.data = append(m.data, idx)
	if m.currentIndexFile == "" {
		m.currentIndexFile = idx.indexFile
	}
	return nil
}

func (m *MessageIndexes) Find(messagePos uint64) *MessageIndex {
	m.RLock()
	defer m.RUnlock()

	foundIdx := sort.Search(len(m.data), func(i int) bool {
		return m.data[i].startMessagePos <= messagePos && m.data[i].stopMessagePos > messagePos
	})
	if foundIdx == len(m.data) {
		return nil
	}
	return m.data[foundIdx]
}

// 索引缓存池。
// IndexPool缓存了消息索引，用于对消息的快速定位查找。其查找流程为：
// 从缓存中找，没找到再判断索引文件是否加载到缓存，如果没有则加载到缓存，并再次从缓存中查找
type IndexPool struct {
	dataPath map[string]string //key: topic, value: file path
	pathLock sync.RWMutex
	data     map[string]*MessageIndexes //key: topic
	sync.RWMutex
}

func (m *IndexPool) RegisterTopicDataPath(topic, path string) {
	m.pathLock.Lock()
	defer m.pathLock.Unlock()
	if m.dataPath == nil {
		m.dataPath = make(map[string]string)
	}
	m.dataPath[topic] = path
}

func (m *IndexPool) GetTopicPath(topic string) string {
	m.pathLock.RLock()
	defer m.pathLock.RUnlock()
	if ret, ok := m.dataPath[topic]; ok {
		return ret
	}
	return ""
}

func (m *IndexPool) FindTopicIndex(topic string, createIfNotExistst bool) *MessageIndexes {
	if createIfNotExistst {
		m.Lock()
		defer m.Unlock()
	} else {
		m.RLock()
		defer m.RUnlock()
	}
	if ret, ok := m.data[topic]; ok {
		return ret
	} else {
		if createIfNotExistst {
			ret = newMessageIndexes(topic)
			m.data[topic] = ret
			return ret
		}
	}
	return nil
}

// 查询消息，返回查询结果（是否找到，所在消息文件，以及消息所在文件的相对位置）
func (m *IndexPool) findPoolIndex(topic string, messagePos uint64) (ret bool, messageFile string, messageFilePos int64) {
	idxes := m.FindTopicIndex(topic, false)
	if idxes == nil {
		return false, "", -1
	}
	idx := idxes.Find(messagePos)
	if idx == nil {
		return false, "", -1
	}
	rec := idx.Find(messagePos)
	if rec == nil {
		return false, "", -1
	}
	return true, idx.indexFile, rec.FilePos
}

func (m *IndexPool) findStorageIndex(topic string, messagePos uint64) (string, error) {
	dataPath := m.GetTopicPath(topic)
	fileList, err := ioutil.ReadDir(dataPath)
	if err != nil {
		return "", err
	}

	fnList := make([]string, 0, len(fileList))
	for _, fn := range fileList {
		if strings.HasSuffix(fn.Name(), ".idx") {
			fnList = append(fnList, fn.Name())
		}
	}
	if len(fnList) > 0 {
		sort.Strings(fnList)
		fn := fmt.Sprintf("%s_%d.idx", topic, messagePos)
		if n := sort.Search(len(fnList), func(i int) bool {
			if i == len(fnList)-1 {
				return fnList[i] <= fn
			}
			return fnList[i] <= fn && fnList[i] >= fn
		}); n == len(fnList) {
			return fnList[len(fnList)-1], nil
		} else {
			return fnList[n], nil
		}
	}
	return "", nil
}

func (m *IndexPool) loadIndex(topic string, indexFile string) error {
	if strings.Index(indexFile, topic) != 0 {
		return fmt.Errorf("topic: %s, but index file: %s", topic, indexFile)
	}
	dataPath := m.GetTopicPath(topic)
	fn := filepath.Join(dataPath, indexFile)
	idx, err := newMessageIndexFromFile(fn)
	if err != nil {
		return err
	}
	idxes := m.FindTopicIndex(topic, true)
	idxes.AddIndex(idx)
	return nil
}

func (m *IndexPool) ReadMessagesFromFile(fn string, filePos int64, limit int) ([]*protocol.MessageConsumer, error) {
	fd, err := os.Open(fn)
	if err != nil {
		return nil, err
	}
	defer fd.Close()
	_, err = fd.Seek(filePos, io.SeekStart)
	if err != nil {
		return nil, err
	}
	buf := bufio.NewReaderSize(fd, 1024*1024*10)
	ret := make([]*protocol.MessageConsumer, 0, limit)
	for i := 0; i < limit; i++ {
		msg := &protocol.MessageConsumer{}
		btsMsgNum := make([]byte, 8)
		_, err = io.ReadFull(buf, btsMsgNum)
		if err != nil {
			return ret, err
		}
		msg.Num = binary.BigEndian.Uint64(btsMsgNum)
		btsMsgSize := make([]byte, 4)
		_, err = io.ReadFull(buf, btsMsgSize)
		if err != nil {
			return ret, err
		}
		msg.Size = binary.BigEndian.Uint32(btsMsgSize)
		if msg.Size < 4 {
			return ret, protocol.NewError(protocol.ErrorCodeInvalidMessageFile, protocol.ErrorMsgInvalidMessageFile)
		}
		if msg.Size > 4 {
			btsData := make([]byte, msg.Size-4)
			_, err = io.ReadFull(buf, btsData)
			if err != nil {
				return ret, err
			}
			msg.Data = btsData
		}
		btsCrc := make([]byte, 4)
		_, err = io.ReadFull(buf, btsCrc)
		if err != nil {
			return ret, err
		}
		msg.Crc = binary.BigEndian.Uint32(btsCrc)
		ret = append(ret, msg)
		bstTailSize := make([]byte, 8)
		_, err = io.ReadFull(buf, bstTailSize)
		if err != nil {
			return ret, err
		}
	}

	return ret, nil
}

func (m *IndexPool) Query(q *protocol.MessageQuery) {
	b, fn, pos := m.findPoolIndex(q.Topic, q.MessagePos)
	var err error
	if !b {
		fn, err = m.findStorageIndex(q.Topic, q.MessagePos)
		if err == nil {
			err = m.loadIndex(q.Topic, fn)
		}
		if err != nil {
			q.Result <- &protocol.MessageQueryResult{Message: nil, Error: err}
			return
		}

	}
	if !b {
		b, fn, pos = m.findPoolIndex(q.Topic, q.MessagePos)
	}

	if b && fn != "" && pos >= 0 {
		path := m.GetTopicPath(q.Topic)
		fullFn := filepath.Join(path, fn)
		msgList, err := m.ReadMessagesFromFile(fullFn, pos, q.Limit)
		for _, v := range msgList {
			q.Result <- &protocol.MessageQueryResult{Message: v, Error: nil}
		}
		if err != nil {
			q.Result <- &protocol.MessageQueryResult{Message: nil, Error: err}
		}
		return
	}

	q.Result <- &protocol.MessageQueryResult{Message: nil, Error: protocol.NewError(protocol.ErrorCodeNoMessage, protocol.ErrorMsgNoMessage)}
}
