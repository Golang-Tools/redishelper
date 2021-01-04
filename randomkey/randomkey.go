/* Package randomkey
随机生成一个key
*/
package randomkey

import (
	"strconv"
	"time"

	"github.com/sony/sonyflake"
)

var generator *sonyflake.Sonyflake

//InitGenerator 初始化默认生成器
func InitGenerator(clientID ...uint16) error {
	if generator != nil {
		return ErrDefaultGeneratorAllreadySetted
	}
	switch len(clientID) {
	case 0:
		{
			setting := sonyflake.Settings{
				StartTime: time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
			}
			generator = sonyflake.NewSonyflake(setting)
		}
	case 1:
		{
			setting := sonyflake.Settings{
				StartTime: time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
				MachineID: func() (uint16, error) {
					return clientID[0], nil
				},
			}
			generator = sonyflake.NewSonyflake(setting)
		}
	default:
		{
			return ErrIndefiniteParameterClientLength
		}
	}
	return nil
}

//NextKey 随机生成key
func NextKey() (string, error) {
	if generator == nil {
		return "", ErrDefaultGeneratorNotSetYet
	}
	id, err := generator.NextID()
	if err != nil {
		return "", err
	}
	return strconv.FormatUint(id, 32), nil
}
