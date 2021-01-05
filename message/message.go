package message

//Message 消息对象
type Message struct {
	Topic   string
	Payload string
}

//NewFromQueue 从queue中获取消息对象
func NewFromQueue(res []string) (*Message, error) {
	if len(res) != 2 {
		return nil, ErrQueueResNotTwo
	}
	m := new(Message)
	m.Topic = res[0]
	m.Payload = res[1]
	return m, nil
}
