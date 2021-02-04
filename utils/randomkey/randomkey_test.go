package randomkey

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_namespace(t *testing.T) {
	err := InitGenerator(uint16(3))
	if err != nil {
		assert.Error(t, err, "InitGenerator error")
	}
	a, err := NextKey()
	if err != nil {
		assert.Error(t, err, "NextKey error")
	}
	b, err := NextKey()
	if err != nil {
		assert.Error(t, err, "NextKey error")
	}
	assert.NotEqual(t, a, b)
}
