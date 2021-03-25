package main

import (
	"errors"
	"fmt"
	"net"
	"reflect"

	jsoniter "github.com/json-iterator/go"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

//Event 消息对象
type Event struct {
	Topic     string      `json:"topic" msgpack:"topic"`
	Sender    string      `json:"sender,omitempty" msgpack:"sender,omitempty"`
	EventTime int64       `json:"event_time,omitempty" msgpack:"event_time,omitempty"`
	EventID   string      `json:"event_id,omitempty" msgpack:"event_id,omitempty"`
	Payload   interface{} `json:"payload" msgpack:"payload"`
}

//Msg 消息对象
type Msg struct {
	A string `json:"a" msgpack:"a"`
	B string `json:"b,omitempty" msgpack:"b,omitempty"`
}

func privateIPv4() (net.IP, error) {
	as, err := net.InterfaceAddrs()
	if err != nil {
		return nil, err
	}

	for _, a := range as {
		ipnet, ok := a.(*net.IPNet)
		if !ok || ipnet.IP.IsLoopback() {
			continue
		}

		ip := ipnet.IP.To4()
		if isPrivateIPv4(ip) {
			return ip, nil
		}
	}
	return nil, errors.New("no private ip address")
}
func isPrivateIPv4(ip net.IP) bool {
	return ip != nil &&
		(ip[0] == 10 || ip[0] == 172 && (ip[1] >= 16 && ip[1] < 32) || ip[0] == 192 && ip[1] == 168)
}

func lower16BitPrivateIP() (uint16, error) {
	ip, err := privateIPv4()
	if err != nil {
		return 0, err
	}

	return uint16(ip[2])<<8 + uint16(ip[3]), nil
}

func main() {
	// fmt.Println(lower16BitPrivateIP())
	m := Msg{A: "a", B: "b"}
	e := Event{
		Topic:     "1",
		Sender:    "2",
		EventTime: 12343,
		EventID:   "123",
		Payload:   &m,
	}
	s, err := json.Marshal(e)
	if err != nil {
		fmt.Println("error", err.Error())
		return
	}
	// s1, err := json.Marshal(m)
	// if err != nil {
	// 	fmt.Println("error", err.Error())
	// 	return
	// }
	m1 := Msg{}
	e1 := Event{
		Payload: &m1,
	}
	t := reflect.TypeOf(&e1).Elem()
	e2 := reflect.New(t)
	err = json.Unmarshal(s, &e2)
	if err != nil {
		fmt.Println("error", err.Error())
		return
	}
	fmt.Println("e2 ", e2)
	fmt.Println("m1 ", m1)

	// err = json.Unmarshal(s1, &e2)
	// if err != nil {
	// 	fmt.Println("error", err.Error())
	// 	return
	// }
	// fmt.Println("e2 ", e2)
	// fmt.Println("m1 ", m1)
}
