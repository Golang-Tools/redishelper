package pchelper

import (
	"strconv"

	jsoniter "github.com/json-iterator/go"
	"github.com/vmihailenco/msgpack/v5"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

//Event 消息对象
type Event struct {
	Topic     string      `json:"topic,omitempty" msgpack:"topic,omitempty"`
	Sender    string      `json:"sender,omitempty" msgpack:"sender,omitempty"`
	EventTime int64       `json:"event_time,omitempty" msgpack:"event_time,omitempty"` //毫秒级时间戳
	EventID   string      `json:"event_id,omitempty" msgpack:"event_id,omitempty"`
	Payload   interface{} `json:"payload" msgpack:"payload"`
}

func defaultCommonParser(SerializeProtocol SerializeProtocolType, topic, payloadstr string) (*Event, error) {
	m := Event{}
	switch SerializeProtocol {
	case SerializeProtocol_JSON:
		{
			err := json.Unmarshal([]byte(payloadstr), &m)
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
			if topic != "" {
				m.Topic = topic
			}
		}
	case SerializeProtocol_MSGPACK:
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
			return nil, ErrUnSupportSerializeProtocol
		}
	}
	return &m, nil
}

func defaultStreamParser(SerializeProtocol SerializeProtocolType, topic, eventID string, payload map[string]interface{}) (*Event, error) {
	m := Event{}
	sender, ok1 := payload["sender"]
	if ok1 {
		m.Sender = sender.(string)
		delete(payload, "sender")
	}
	et, ok2 := payload["event_time"]
	if ok2 {
		etimestr := et.(string)
		etime, err := strconv.ParseInt(etimestr, 10, 64)
		if err != nil {
			return nil, err
		}
		m.EventTime = etime
		delete(payload, "event_time")
	}
	res := map[string]interface{}{}
	p, ok3 := payload["payload"]
	if ok3 {
		payloadStr := p.(string)
		switch SerializeProtocol {
		case SerializeProtocol_JSON:
			{
				err := json.UnmarshalFromString(payloadStr, &res)

				if err != nil || len(res) == 0 {
					switch payloadStr {
					case "true":
						{
							res["payload"] = true
						}
					case "false":
						{
							res["payload"] = false
						}
					default:
						{
							pli, err := strconv.ParseInt(payloadStr, 10, 64)
							if err != nil {
								plf, err := strconv.ParseFloat(payloadStr, 64)
								if err != nil {
									res["payload"] = payloadStr
								} else {
									res["payload"] = plf
								}
							} else {
								res["payload"] = pli
							}
						}
					}
				}
			}
		case SerializeProtocol_MSGPACK:
			{
				err := msgpack.Unmarshal([]byte(payloadStr), &res)
				if err != nil || len(res) == 0 {
					switch payloadStr {
					case "true":
						{
							res["payload"] = true
						}
					case "false":
						{
							res["payload"] = false
						}
					default:
						{
							pli, err := strconv.ParseInt(payloadStr, 10, 64)
							if err != nil {
								plf, err := strconv.ParseFloat(payloadStr, 64)
								if err != nil {
									res["payload"] = payloadStr
								} else {
									res["payload"] = plf
								}
							} else {
								res["payload"] = pli
							}
						}
					}
				}
			}
		default:
			{
				return nil, ErrUnSupportSerializeProtocol
			}
		}
		delete(payload, "payload")
	}
	m.Topic = topic
	m.EventID = eventID
	for key, value := range payload {
		vstr := value.(string)
		// log.Info("defaultStreamParser get pair", log.Dict{"key": key, "value": vstr})
		valueM := map[string]interface{}{}
		switch SerializeProtocol {
		case SerializeProtocol_JSON:
			{
				err := json.UnmarshalFromString(vstr, &valueM)
				if err != nil || len(valueM) == 0 {
					switch vstr {
					case "true":
						{
							res[key] = true
						}
					case "false":
						{
							res[key] = false
						}
					default:
						{
							vi, err := strconv.ParseInt(vstr, 10, 64)
							if err != nil {
								vf, err := strconv.ParseFloat(vstr, 64)
								if err != nil {
									res[key] = vstr
								} else {
									res[key] = vf
								}
							} else {
								res[key] = vi
							}
						}
					}
				} else {
					res[key] = valueM
				}
			}
		case SerializeProtocol_MSGPACK:
			{
				err := msgpack.Unmarshal([]byte(vstr), &valueM)
				if err != nil || len(valueM) == 0 {
					switch vstr {
					case "true":
						{
							res[key] = true
						}
					case "false":
						{
							res[key] = false
						}
					default:
						{
							vi, err := strconv.ParseInt(vstr, 10, 64)
							if err != nil {
								vf, err := strconv.ParseFloat(vstr, 64)
								if err != nil {
									res[key] = vstr
								} else {
									res[key] = vf
								}
							} else {
								res[key] = vi
							}
						}
					}
				} else {
					res[key] = valueM
				}
			}
		default:
			{
				return nil, ErrUnSupportSerializeProtocol
			}
		}
	}
	m.Payload = res
	return &m, nil
}

//DefaultParser 默认的消息处理函数负载会被解析为 m,ap[string]interface{}
func DefaultParser(SerializeProtocol SerializeProtocolType, topic, eventID, payloadstr string, payload map[string]interface{}) (*Event, error) {
	if eventID == "" {
		return defaultCommonParser(SerializeProtocol, topic, payloadstr)
	}
	return defaultStreamParser(SerializeProtocol, topic, eventID, payload)
}
