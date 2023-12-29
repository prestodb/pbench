package log_test

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"net"
	"presto-benchmark/log"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLog(t *testing.T) {
	b := new(strings.Builder)
	log.SetGlobalLogger(log.Output(b))
	log.Log().Str("key", "value").Bool("yes", true).Int("num", 24).
		Msgf("hello %d %s", 12, "world")
	log.Info().Msg("another line")
	assert.Equal(t, `{"key":"value","yes":true,"num":24,"message":"hello 12 world"}
{"level":"info","message":"another line"}`+"\n", b.String(),
		"incorrect log output, check if \"yzhang.io/internal\" is imported in main.go")
}

func TestFatalOverride(t *testing.T) {
	b := new(strings.Builder)
	log.SetGlobalLogger(log.Output(b))
	log.OverrideFatal = true
	defer func() {
		log.OverrideFatal = false
	}()
	log.Fatal().Err(errors.New("test error")).Send()
	assert.Equal(t, `{"level":"fatal","error":"test error"}`+"\n", b.String())
}

func TestLoggerInContext(t *testing.T) {
	b := new(strings.Builder)
	logger := log.Output(b)
	ctx := logger.WithContext(context.Background())
	log.Ctx(ctx).Debug().Msg("test log line")
	assert.Equal(t, "{\"level\":\"debug\",\"message\":\"test log line\"}\n", b.String())
}

func TestLogStringArray(t *testing.T) {
	b := new(strings.Builder)
	logger := log.Output(b)
	arr := []string{"a", "b", "c", "d\ne"}
	logger.Info().Array("arr", log.NewArrayMarshaller(arr)).Send()
	assert.Equal(t, `{"level":"info","arr":["a","b","c","d\ne"]}`+"\n", b.String())
}

func TestLogStringMap(t *testing.T) {
	b := new(strings.Builder)
	logger := log.Output(b)
	m := map[string]string{
		"name":   "Tom",
		"friend": "Jerry",
	}
	logger.Info().Object("map", log.StringMapMarshaller(m)).Msg("test map")
	assert.Equal(t, `{"level":"info","map":{"friend":"Jerry","name":"Tom"},"message":"test map"}`+"\n", b.String())
}

type A struct {
	id int
}

func (a *A) String() string {
	return "struct A"
}

type B struct {
	pai float32
}

func (a *B) String() string {
	return "struct B"
}

func TestLogArray(t *testing.T) {
	b := new(strings.Builder)
	logger := log.Output(b)
	log.MaskPointerValueForTesting = true
	defer func() {
		log.MaskPointerValueForTesting = false
	}()
	intSlice := []int{1, 2, 3, 4, 5}
	stringArray := [3]string{"first", "second", "third"}
	mixedTypeSlice := []any{uint(8), int8(12), uint64(10086), "a string", true, 3.1415926, struct {
		name      string
		price     float32
		quantity  int
		discount  bool
		error     error
		arr       []fmt.Stringer
		customers map[uint]string
	}{
		name:      "an item",
		price:     3.14,
		quantity:  6,
		discount:  true,
		error:     errors.New("some interesting error"),
		arr:       []fmt.Stringer{&A{id: 101}, &B{pai: 3.14}},
		customers: map[uint]string{12: "Zhang", 13: "Sun", 24: "Li", 96: "Zhou"},
	}}
	ips := []net.IP{net.ParseIP("192.168.1.1")}
	logger.Info().Array("intSlice", log.NewArrayMarshaller(intSlice)).Send()
	logger.Info().Array("stringArray", log.NewArrayMarshaller(stringArray)).Send()
	logger.Info().Array("mixedTypeSlice", log.NewArrayMarshaller(mixedTypeSlice)).Send()
	logger.Info().Array("IP", log.NewArrayMarshaller(ips)).Send()
	expected := `{"level":"info","intSlice":[1,2,3,4,5]}
{"level":"info","stringArray":["first","second","third"]}
{"level":"info","mixedTypeSlice":[8,12,10086,"a string",true,3.1415926,{"name":"an item","price":3.14,"quantity":6,"discount":true,"error":{"msg":"some interesting error","stack":[0,0,0]},"arr":[{"id":101},{"pai":3.14}],"customers":{"type":"map","value":"map[12:Zhang 13:Sun 24:Li 96:Zhou]"}}]}
{"level":"info","IP":[{"type":"slice","value":"192.168.1.1"}]}
`
	assert.Equal(t, expected, b.String())
}

func TestLogStruct(t *testing.T) {
	b := new(strings.Builder)
	logger := log.Output(b)
	item := struct {
		name     string
		price    float32
		quantity uint16
		discount bool
		orders   []int
		company  struct {
			name    string
			address string
			open    bool
		}
	}{
		name:     "an item",
		price:    3.14,
		quantity: 6,
		discount: true,
		orders:   []int{6001, 6002, 6003},
		company: struct {
			name    string
			address string
			open    bool
		}{name: "Banana", address: "12345 Xyz Rd", open: true},
	}
	logger.Info().Object("item", log.NewObjectMarshaller(item)).Send()
	assert.Equal(t, `{"level":"info","item":{"name":"an item","price":3.14,"quantity":6,"discount":true,"orders":[6001,6002,6003],"company":{"name":"Banana","address":"12345 Xyz Rd","open":true}}}
`, b.String())
}

func TestMarshalArrayAsObject(t *testing.T) {
	str := "hello world"
	orders := []any{6001, "6002", 6003, &str, struct {
		price float32
		desc  fmt.Stringer
	}{price: 3.14}}
	b := new(strings.Builder)
	var i any = &orders
	logger := log.Output(b)
	logger.Info().Object("obj", log.NewObjectMarshaller(i)).Send()
	assert.Equal(t, `{"level":"info","obj":{"array":[6001,"6002",6003,"hello world",{"price":3.14}]}}`+"\n", b.String())
}
