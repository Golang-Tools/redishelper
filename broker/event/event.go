package event

import (
	"encoding/json"
	"strconv"

	"github.com/Golang-Tools/redishelper/broker"
	"github.com/vmihailenco/msgpack/v5"
)

//Event 消息对象
type Event struct {
	Topic     string      `json:"topic,omitempty" msgpack:"topic"`
	Sender    string      `json:"sender,omitempty" msgpack:"sender,omitempty"`
	EventTime int64       `json:"event_time,omitempty" msgpack:"event_time,omitempty"`
	EventID   string      `json:"event_id,omitempty" msgpack:"event_id,omitempty"`
	Payload   interface{} `json:"payload" msgpack:"payload"`
}

//Handdler 处理消息的回调函数
//@params msg *Event Event对象
type Handdler func(msg *Event) error

//Parser 用于将负载字符串转化为event的函数
//规定eventID不为""时解析流的消息,用到topic, eventID, payload
//规定eventID为""时解析除流之外的消息,用到SerializeProtocol,topic, payloadstr
type Parser func(SerializeProtocol, topic, eventID, payloadstr string, payload map[string]interface{}) (*Event, error)

//DefaultParser 默认的消息处理函数负载会被解析为 map[string]interface{}
func DefaultParser(SerializeProtocol, topic, eventID, payloadstr string, payload map[string]interface{}) (*Event, error) {
	m := Event{}
	if eventID == "" {
		switch SerializeProtocol {
		case "JSON":
			{
				err := json.Unmarshal([]byte(payloadstr), &m)
				if err != nil {
					if err != nil || m.EventTime == 0 {
						// log.Error("default parser message error 1", log.Dict{"err": err})
						p := map[string]interface{}{}
						err := json.Unmarshal([]byte(payloadstr), &p)
						if err != nil {
							// log.Error("default parser message error 2", log.Dict{"err": err})
							payloadStr := string(payloadstr)
							switch payloadStr {
							case "true":
								{
									m.Payload = true
								}
							case "false":
								{
									m.Payload = false
								}
							default:
								{
									pli, err := strconv.ParseInt(payloadStr, 10, 64)
									if err != nil {
										plf, err := strconv.ParseFloat(payloadStr, 64)
										if err != nil {
											m.Payload = payloadStr
										} else {
											m.Payload = plf
										}
									} else {
										m.Payload = pli
									}
								}
							}

						} else {
							m.Payload = p
						}
					}
				}
				if topic != "" {
					m.Topic = topic
				}
			}
		case "msgpack":
			{
				err := msgpack.Unmarshal([]byte(payloadstr), &m)
				if err != nil || m.EventID == "" {
					p := map[string]interface{}{}
					err := json.Unmarshal([]byte(payloadstr), &p)
					if err != nil {
						payloadStr := string(payloadstr)
						switch payloadStr {
						case "true":
							{
								m.Payload = true
							}
						case "false":
							{
								m.Payload = false
							}
						default:
							{
								pli, err := strconv.ParseInt(payloadStr, 10, 64)
								if err != nil {
									plf, err := strconv.ParseFloat(payloadStr, 64)
									if err != nil {
										m.Payload = payloadStr
									} else {
										m.Payload = plf
									}
								} else {
									m.Payload = pli
								}
							}
						}
					} else {
						m.Payload = p
					}
				}
				if topic != "" {
					m.Topic = topic
				}
			}
		default:
			{
				return nil, broker.ErrUnSupportSerializeProtocol
			}
		}
	} else {
		sender, ok1 := payload["sender"]
		if ok1 {
			m.Sender = sender.(string)
			delete(payload, "sender")
		}
		et, ok2 := payload["event_time"]
		if ok2 {
			m.EventTime = et.(int64)
			delete(payload, "event_time")
		}
		m.Topic = topic
		m.EventID = eventID
		m.Payload = payload
	}
	return &m, nil
}
