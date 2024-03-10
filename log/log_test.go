package log_test

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"net"
	"os"
	"pbench/log"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	// Stop the logger from printing out the timestamp, so we can make the test output comparable.
	log.SetGlobalLogger(zerolog.New(os.Stdout))
	log.MaskPointerValueForTesting = true
	defer func() {
		log.MaskPointerValueForTesting = false
	}()
	os.Exit(m.Run())
}

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
	logger.Info().Array("arr", log.NewMarshaller(arr)).Send()
	assert.Equal(t, `{"level":"info","arr":["a","b","c","d\ne"]}`+"\n", b.String())
}

func TestLogStringMap(t *testing.T) {
	b := new(strings.Builder)
	logger := log.Output(b)
	m := map[string]string{
		"name":   "Tom",
		"friend": "Jerry",
	}
	logger.Info().Object("map", log.NewMarshaller(m)).Msg("test map")
	assert.Equal(t, `{"level":"info","map":{"friend":"Jerry","name":"Tom"},"message":"test map"}`+"\n", b.String())
}

func TestMultiTypeMap(t *testing.T) {
	b := new(strings.Builder)
	logger := log.Output(b)
	m := map[any]any{
		"name":       "Tom",
		"married":    true,
		123:          []any{"a", "c", 16, 3.2},
		false:        44,
		"SnakeField": float32(10.555),
	}
	logger.Info().Object("map", log.NewMarshaller(m)).Msg("test multi-type map")
	assert.Equal(t, `{"level":"info","map":{"123":["a","c",16,3.2],"snake_field":10.555,"false":44,"married":true,"name":"Tom"},"message":"test multi-type map"}`+"\n", b.String())
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
	logger.Info().Array("intSlice", log.NewMarshaller(intSlice)).Send()
	logger.Info().Array("stringArray", log.NewMarshaller(stringArray)).Send()
	logger.Info().Array("mixedTypeSlice", log.NewMarshaller(mixedTypeSlice).SetNestedLevelLimit(4)).Send()
	logger.Info().Array("IP", log.NewMarshaller(ips).SetNestedLevelLimit(3)).Send()
	expected := `{"level":"info","intSlice":[1,2,3,4,5]}
{"level":"info","stringArray":["first","second","third"]}
{"level":"info","mixedTypeSlice":[8,12,10086,"a string",true,3.1415926,{"name":"an item","price":3.14,"quantity":6,"discount":true,"error":{"msg":"some interesting error","stack":[0,0,0]},"arr":[{"id":101},{"pai":3.14}],"customers":{"12":"Zhang","13":"Sun","24":"Li","96":"Zhou"}}]}
{"level":"info","IP":["192.168.1.1"]}
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
	logger.Info().Object("item", log.NewMarshaller(item)).Send()
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
	logger.Info().Object("obj", log.NewMarshaller(i)).Send()
	assert.Equal(t, `{"level":"info","obj":{"array":[6001,"6002",6003,"hello world",{"price":3.14}]}}`+"\n", b.String())
}

type NestedStruct struct {
	Info      string
	Map       map[string]any
	NextLevel *NestedStruct
	Note      string
	Remark    string
	Count     int
	Sum       float64
	Avg       float64
}

func TestLimit(t *testing.T) {
	obj := &NestedStruct{
		Info: "level 1",
		NextLevel: &NestedStruct{
			Info: "level 2",
			Map: map[string]any{
				"another map": map[string]any{
					"nested struct": &NestedStruct{
						Info:      "struct inside a map",
						NextLevel: nil,
					},
					"2nd level map": map[int]string{
						2:  "two",
						10: "ten",
					},
				},
				"passed": true,
				"structArr": []*NestedStruct{
					{
						Info: "level 3",
						NextLevel: &NestedStruct{
							Info: "level 4",
						},
					},
				},
			},
			NextLevel: &NestedStruct{
				Info: "level 3",
				NextLevel: &NestedStruct{
					Info:      "level 4",
					NextLevel: nil,
				},
			},
		},
		Map: map[string]any{
			"an array":   []int{1, 2, 3, 4, 5, 6, 7, 8},
			"id":         15,
			"price":      12.3,
			"comment":    "this is very good",
			"caption":    "Holding Hands",
			"singer":     "Julie Sue",
			"url":        "https://youtu.be/wJNHRweW5WE?si=uphC3qgUMEi-C3M2",
			"liked":      false,
			"thumbs_ups": 22000,
		},
		Note: "nothing to note",
	}
	b := new(strings.Builder)
	logger := log.Output(b)
	logger.Info().Object("obj", log.NewMarshaller(obj).
		SetNestedLevelLimit(3).
		SetFieldOrElementLimit(7)).Send()
	assert.Equal(t, `{"level":"info","obj":{"info":"level 1","map":{"an array":[1,2,3,4,5,6,7,"..."],"caption":"Holding Hands","comment":"this is very good","id":15,"liked":false,"price":12.3,"singer":"Julie Sue","...":"<map truncated>"},"next_level":{"info":"level 2","map":{"another map":"<map[string]interface {} Value>","passed":true,"struct_arr":"<[]*log_test.NestedStruct Value>"},"next_level":{"info":"level 3","map":"<map[string]interface {} Value>","next_level":"<log_test.NestedStruct Value>","note":"","remark":"","count":0,"sum":0,"...":"<field truncated>"},"note":"","remark":"","count":0,"sum":0,"...":"<field truncated>"},"note":"nothing to note","remark":"","count":0,"sum":0,"...":"<field truncated>"}}`+"\n", b.String())
}
