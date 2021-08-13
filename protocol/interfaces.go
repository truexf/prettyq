package protocol

type Storage interface {
	SaveMessage(msg *MessageStorage) error
	QueryMessage(query *MessageQuery)
}
