package randomkey

import (
	"testing"
	"time"

	"github.com/sony/sonyflake"
	"github.com/stretchr/testify/assert"
)

func Test_gen_key_without_init(t *testing.T) {
	res1, err := Next()
	if err != nil {
		assert.FailNow(t, err.Error(), "nextKeynot get error")
	}
	res2, err := Next()
	if err != nil {
		assert.FailNow(t, err.Error(), "nextKeynot get error")
	}
	assert.NotEqual(t, res1, res2)
}

func Test_gen_key(t *testing.T) {
	InitGenerator(sonyflake.Settings{
		StartTime: time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC),
		MachineID: func() (uint16, error) {
			return uint16(14), nil
		},
	})
	a, err := Next()
	if err != nil {
		assert.FailNow(t, err.Error(), "nextKeynot get error")
	}
	b, err := Next()
	if err != nil {
		assert.FailNow(t, err.Error(), "nextKeynot get error")
	}
	assert.NotEqual(t, a, b)
}
