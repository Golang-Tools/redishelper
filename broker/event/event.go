package event

//Event 消息对象
type Event struct {
	Topic     string      `json:"topic" msgpack:"topic"`
	Sender    string      `json:"sender,omitempty" msgpack:"sender,omitempty"`
	EventTime int64       `json:"event_time,omitempty" msgpack:"event_time,omitempty"`
	EventID   string      `json:"event_id,omitempty" msgpack:"event_id,omitempty"`
	Payload   interface{} `json:"payload" msgpack:"payload"`
}

//Handdler 处理消息的回调函数
//@params msg *Message Message对象
type Handdler func(msg *Event) error
