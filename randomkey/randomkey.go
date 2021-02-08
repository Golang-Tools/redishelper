//Package randomkey 随机生成一个key
//使用sony实现的snowflake算法
//默认的设置为 StartTime: time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),MachineID则会默认尝试使用本机的第一张网卡的mac地址最后1位,如果获取不到则会使用默认值0
package randomkey

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/sony/sonyflake"
)

var defaultSetting = sonyflake.Settings{
	StartTime: time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
	MachineID: func() (uint16, error) {
		netInterfaces, err := net.Interfaces()
		if err != nil {
			return uint16(0), nil
		}
		macAddr := ""
		for _, netInterface := range netInterfaces {
			mac := netInterface.HardwareAddr.String()
			if len(mac) == 0 {
				continue
			}
			macAddr = mac
		}
		if macAddr == "" {
			return uint16(0), nil
		}
		fmt.Println(macAddr)
		b := strings.Split(macAddr, ":")
		value, err := strconv.ParseUint(b[len(b)-1], 16, 16)
		if err != nil {
			return uint16(0), nil
		}
		value2 := uint16(value)
		return value2, nil
	},
}
var generator *sonyflake.Sonyflake = sonyflake.NewSonyflake(defaultSetting)

//InitGenerator 初始化默认生成器
func InitGenerator(opt sonyflake.Settings) *sonyflake.Sonyflake {
	generator = sonyflake.NewSonyflake(opt)
	return generator
}

//Next 随机生成key
func Next() (string, error) {
	id, err := generator.NextID()
	if err != nil {
		return "", err
	}
	return strconv.FormatUint(id, 32), nil
}
