package pchelper

import (
	"errors"
	"fmt"
	"reflect"

	msgpack "github.com/vmihailenco/msgpack/v5"
)

func ToBytes(spt SerializeProtocolType, payload interface{}) ([]byte, error) {
	switch payload := payload.(type) {
	case string:
		{
			return []byte(payload), nil
		}
	case []byte:
		{
			return payload, nil
		}
	case bool:
		{
			if payload {
				return []byte("true"), nil
			} else {
				return []byte("false"), nil
			}
		}
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		{
			return []byte(fmt.Sprintf("%d", payload)), nil
		}

	case float32, float64, complex64, complex128:
		{
			return []byte(fmt.Sprintf("%f", payload)), nil
		}
	default:
		{
			switch spt {
			case SerializeProtocol_JSON:
				{
					return json.Marshal(payload)

				}
			case SerializeProtocol_MSGPACK:
				{
					return msgpack.Marshal(payload)
				}
			default:
				{
					return nil, ErrUnSupportSerializeProtocol
				}
			}
		}
	}
}

//pasmap 解析map
func pasmap(spt SerializeProtocolType, pl map[string]interface{}) (map[string]interface{}, error) {
	result := map[string]interface{}{}
	for key, value := range pl {
		v := reflect.ValueOf(value)
		switch v.Kind() {
		case reflect.Bool:
			{
				if value.(bool) {
					result[key] = "true"
				} else {
					result[key] = "false"
				}
			}
		case reflect.Int, reflect.Int8, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint32, reflect.Uint64:
			{
				result[key] = value
			}
		case reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128:
			{
				result[key] = value
			}
		case reflect.String:
			{
				result[key] = value
			}
		case reflect.Slice:
			{
				_, ok := value.([]byte)
				if ok {
					result[key] = value
				} else {
					switch spt {
					case SerializeProtocol_JSON:
						{
							payloadstr, err := json.MarshalToString(value)
							if err != nil {
								return nil, err
							}
							result[key] = payloadstr
						}
					case SerializeProtocol_MSGPACK:
						{
							payloadBytes, err := msgpack.Marshal(value)
							if err != nil {
								return nil, err
							}
							result[key] = string(payloadBytes)
						}
					default:
						{
							return nil, ErrUnSupportSerializeProtocol
						}
					}
				}
			}
		case reflect.Map:
			{
				switch spt {
				case SerializeProtocol_JSON:
					{
						payloadstr, err := json.MarshalToString(value)
						if err != nil {
							return nil, err
						}
						result[key] = payloadstr
					}
				case SerializeProtocol_MSGPACK:
					{
						payloadBytes, err := msgpack.Marshal(value)
						if err != nil {
							return nil, err
						}
						result[key] = string(payloadBytes)
					}
				default:
					{
						return nil, ErrUnSupportSerializeProtocol
					}
				}
			}
		case reflect.Chan:
			{
				return nil, errors.New("not support chan as payload")
			}
		default:
			{
				switch spt {
				case SerializeProtocol_JSON:
					{
						payloadstr, err := json.MarshalToString(value)
						if err != nil {
							return nil, err
						}
						result[key] = payloadstr
					}
				case SerializeProtocol_MSGPACK:
					{
						payloadBytes, err := msgpack.Marshal(value)
						if err != nil {
							return nil, err
						}
						result[key] = string(payloadBytes)
					}
				default:
					{
						return nil, ErrUnSupportSerializeProtocol
					}
				}
			}
		}
	}
	return result, nil
}

func ToXAddArgsValue(spt SerializeProtocolType, payload interface{}) (interface{}, error) {
	var Values interface{}
	v := reflect.ValueOf(payload)
	switch v.Kind() {
	case reflect.Bool:
		{
			if payload.(bool) {
				Values = map[string]interface{}{"value": "true"}
			} else {
				Values = map[string]interface{}{"value": "false"}
			}
		}
	case reflect.Int, reflect.Int8, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint32, reflect.Uint64:
		{
			Values = map[string]interface{}{"value": payload}
		}
	case reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128:
		{
			Values = map[string]interface{}{"value": payload}
		}
	case reflect.String:
		{
			Values = map[string]interface{}{"value": payload}
		}
	case reflect.Slice:
		{
			payloadb, ok := payload.([]byte)
			if ok {
				Values = map[string]interface{}{"value": string(payloadb)}
			} else {
				return nil, ErrNotSupportSliceAsPayload
			}
		}
	case reflect.Map:
		{
			pl, ok := payload.(map[string]interface{})
			if !ok {
				return nil, ErrMapPayloadCanNotCast
			}
			res, err := pasmap(spt, pl)
			if err != nil {
				return nil, err
			}
			Values = res
		}
	case reflect.Chan:
		{
			return nil, ErrNotSupportChanAsPayload
		}
	default:
		{
			mm := map[string]interface{}{}
			switch spt {
			case SerializeProtocol_JSON:
				{
					payloadBytes, err := json.Marshal(payload)
					if err != nil {
						return nil, err
					}
					err = json.Unmarshal(payloadBytes, &mm)
					if err != nil {
						return nil, err
					}
					res, err := pasmap(spt, mm)
					if err != nil {
						return nil, err
					}
					Values = res
				}
			case SerializeProtocol_MSGPACK:
				{
					payloadBytes, err := msgpack.Marshal(payload)
					if err != nil {
						return nil, err
					}
					err = msgpack.Unmarshal(payloadBytes, &mm)
					if err != nil {
						return nil, err
					}
					res, err := pasmap(spt, mm)
					if err != nil {
						return nil, err
					}
					Values = res
				}
			default:
				{
					return nil, ErrUnSupportSerializeProtocol
				}
			}
		}
	}
	return Values, nil
}
