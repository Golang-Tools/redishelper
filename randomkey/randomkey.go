//Package randomkey 随机生成一个key
//使用sony实现的snowflake算法
//默认的设置为 StartTime: time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),MachineID则会默认尝试使用本机的第一张网卡的mac地址最后1位,如果获取不到则会使用默认值0
package randomkey

import (
	"errors"
	"net"
	"strconv"
	"time"

	"github.com/sony/sonyflake"
)

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

var defaultSetting = sonyflake.Settings{
	StartTime: time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
}

var generator *sonyflake.Sonyflake = sonyflake.NewSonyflake(defaultSetting)

//machineID 当前机器的id字符串
var machineID uint16 = 0

//InitGenerator 初始化默认生成器
func InitGenerator(opt sonyflake.Settings) *sonyflake.Sonyflake {
	mID, err := opt.MachineID()
	if err != nil {
		opt.MachineID = nil
	}
	machineID = mID
	generator = sonyflake.NewSonyflake(opt)
	return generator
}

//GetMachineID 获取生成器的MachineID
func GetMachineID() uint16 {
	if machineID == 0 {
		mID, err := lower16BitPrivateIP()
		if err != nil {
			return machineID
		}
		machineID = mID
	}
	return machineID
}

//Next 随机生成key
func Next() (string, error) {
	id, err := generator.NextID()
	if err != nil {
		return "", err
	}
	return strconv.FormatUint(id, 32), nil
}
