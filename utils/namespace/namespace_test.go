package namespace

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_namespace(t *testing.T) {
	namespace := NameSpcae{}
	namespace = append(namespace, "a")
	namespace = append(namespace, "b")
	namespace = append(namespace, "c")
	assert.Equal(t, "a::b::c", namespace.String())
	namespacex := namespace[1:]
	assert.Equal(t, "b::c", namespacex.String())
	assert.Equal(t, "b::c::q", namespacex.Key("q"))
	assert.Equal(t, "b::c::q-w-e", namespacex.Key("q", "w", "e"))
	assert.Equal(t, "b::c", namespacex.Key())
}
