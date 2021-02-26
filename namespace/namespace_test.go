package namespace

import (
	"testing"

	"github.com/Golang-Tools/redishelper/exception"
	"github.com/stretchr/testify/assert"
)

func Test_namespace_to_str(t *testing.T) {
	namespace := NameSpcae{"a", "b", "c"}
	ns, err := namespace.ToString()
	if err != nil {
		assert.FailNow(t, err.Error(), "ToString get error")
	}
	assert.Equal(t, "a::b::c", ns)
}

func Test_namespace_to_str_with_delimiter(t *testing.T) {
	namespace := NameSpcae{"a", "b", "c"}
	ns, err := namespace.ToString("??")
	if err != nil {
		assert.FailNow(t, err.Error(), "ToString get error")
	}
	assert.Equal(t, "a??b??c", ns)
}

func Test_namespace_to_str_with_empty_delimiter(t *testing.T) {
	namespace := NameSpcae{"a", "b", "c"}
	_, err := namespace.ToString("")
	if err != nil {
		assert.Equal(t, ErrrParamDelimiterCannotEmpty, err)
	} else {
		assert.FailNow(t, "can not get error")
	}

}

func Test_namespace_to_str_with_multi_delimiter(t *testing.T) {
	namespace := NameSpcae{"a", "b", "c"}
	_, err := namespace.ToString(":", ":")
	if err != nil {
		assert.Equal(t, ErrParamDelimiterLengthMustLessThan2, err)
	} else {
		assert.FailNow(t, "can not get error")
	}
}

func Test_namespace_genkey(t *testing.T) {
	namespace := NameSpcae{"a", "b", "c"}
	k, err := namespace.Key("q", "w")
	if err != nil {
		assert.FailNow(t, err.Error(), "Gen Key get error")
	}
	assert.Equal(t, "a::b::c::q-w", k)
}

func Test_namespace_genkey_without_endpoint(t *testing.T) {
	namespace := NameSpcae{"a", "b", "c"}
	k, err := namespace.Key()
	if err != nil {
		assert.FailNow(t, err.Error(), "Gen Key get error")
	}
	assert.Equal(t, "a::b::c", k)

}
func Test_namespace_genkey_with_empty_endpoint(t *testing.T) {
	namespace := NameSpcae{"a", "b", "c"}
	_, err := namespace.Key("")
	if err != nil {
		assert.Equal(t, ErrrParamEndpointCannotEmpty, err)
	} else {
		assert.FailNow(t, "can not get error")
	}
}

func Test_namespace_genkey_with_delimiter(t *testing.T) {
	namespace := NameSpcae{"a", "b", "c"}
	k, err := namespace.KeyWithDelimiter(&DelimiterOpt{
		NamespaceDelimiter:          ":",
		NamespaceEndpointsDelimiter: "::",
		EndpointsDelimiter:          "_",
	}, "q", "w")
	if err != nil {
		assert.FailNow(t, err.Error(), "Gen Key get error")
	}
	assert.Equal(t, "a:b:c::q_w", k)
}
func Test_namespace_genkey_with_default_delimiter(t *testing.T) {
	namespace := NameSpcae{"a", "b", "c"}
	k, err := namespace.KeyWithDelimiter(&DelimiterOpt{}, "q", "w")
	if err != nil {
		assert.FailNow(t, err.Error(), "Gen Key get error")
	}
	assert.Equal(t, "a::b::c::q-w", k)
	k, err = namespace.KeyWithDelimiter(nil, "q", "w")
	if err != nil {
		assert.FailNow(t, err.Error(), "Gen Key get error")
	}
	assert.Equal(t, "a::b::c::q-w", k)
}

func Test_namespace_genkey_with_delimiter_with_empty_endpoint(t *testing.T) {
	namespace := NameSpcae{"a", "b", "c"}
	_, err := namespace.KeyWithDelimiter(&DelimiterOpt{
		NamespaceDelimiter:          ":",
		NamespaceEndpointsDelimiter: "::",
		EndpointsDelimiter:          "_",
	}, "", "w")
	if err != nil {
		assert.Equal(t, ErrrParamEndpointCannotEmpty, err)
	} else {
		assert.FailNow(t, "can not get error")
	}
}
func Test_namespace_genkey_with_delimiter_without_endpoint(t *testing.T) {
	namespace := NameSpcae{"a", "b", "c"}
	k, err := namespace.KeyWithDelimiter(&DelimiterOpt{
		NamespaceDelimiter:          ":",
		NamespaceEndpointsDelimiter: "::",
		EndpointsDelimiter:          "_",
	})
	if err != nil {
		assert.FailNow(t, err.Error(), "Gen Key get error")
	}
	assert.Equal(t, "a:b:c", k)
}

func Test_namespace_fromkey(t *testing.T) {
	keyStr := "a::b::c"
	namespace, endpointStr, err := FromKeyStr(keyStr)
	if err != nil {
		assert.FailNow(t, err.Error(), "Gen namespace from key string get error")
	}
	assert.Equal(t, NameSpcae{"a", "b"}, namespace)
	assert.Equal(t, "c", endpointStr)
}

func Test_namespace_fromkey_with_delimiter_option(t *testing.T) {
	keyStr := "a:b::c"
	namespace, endpointStr, err := FromKeyStr(keyStr, &DelimiterOpt{
		NamespaceDelimiter:          ":",
		NamespaceEndpointsDelimiter: "::",
	})
	if err != nil {
		assert.FailNow(t, err.Error(), "Gen namespace from key string get error")
	}
	assert.Equal(t, NameSpcae{"a", "b"}, namespace)
	assert.Equal(t, "c", endpointStr)
}

func Test_namespace_fromkey_with_empty_delimiter_option(t *testing.T) {
	keyStr := "a::b::c"
	namespace, endpointStr, err := FromKeyStr(keyStr, nil)
	if err != nil {
		assert.FailNow(t, err.Error(), "Gen namespace from key string get error")
	}
	assert.Equal(t, NameSpcae{"a", "b"}, namespace)
	assert.Equal(t, "c", endpointStr)
}

func Test_namespace_fromkey_with_multi_delimiter_option(t *testing.T) {
	keyStr := "a::b::c"
	_, _, err := FromKeyStr(keyStr, &DelimiterOpt{}, &DelimiterOpt{})
	if err != nil {
		assert.Equal(t, exception.ErrParamOptsLengthMustLessThan2, err)

	} else {
		assert.FailNow(t, "can not get error")
	}
}

func Test_namespace_fromkey_with_no_namespace(t *testing.T) {
	keyStr := "abc"
	_, _, err := FromKeyStr(keyStr)
	if err != nil {
		assert.Equal(t, ErrKeyNotHaveNamespace, err)

	} else {
		assert.FailNow(t, "can not get error")
	}
	_, _, err = FromKeyStr(keyStr, &DelimiterOpt{
		NamespaceDelimiter:          ":",
		NamespaceEndpointsDelimiter: "::",
	})
	if err != nil {
		assert.Equal(t, ErrKeyNotHaveNamespace, err)

	} else {
		assert.FailNow(t, "can not get error")
	}
}

func Test_namespace_fromkey_with_multi_namespace(t *testing.T) {
	keyStr := "a:b::c::d"
	_, _, err := FromKeyStr(keyStr, &DelimiterOpt{
		NamespaceDelimiter:          ":",
		NamespaceEndpointsDelimiter: "::",
	})
	if err != nil {
		assert.Equal(t, ErrKeyParserNamespaceNumberNot2, err)
	} else {
		assert.FailNow(t, "can not get error")
	}
}
