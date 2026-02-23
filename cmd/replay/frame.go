package replay

import (
	"errors"
	"pbench/log"
	"strconv"
	"strings"
	"time"
)

const CreateTimeFormat = "2006-01-02 15:04:05.000 MST"

var ErrInvalidSessionParams = errors.New("invalid session params")

type QueryFrame struct {
	QueryId           string
	CreateTime        time.Time
	WallTimeMillis    int
	OutputRows        int
	WrittenOutputRows int
	Catalog           string
	Schema            string
	SessionProperties string
	Query             string
}

func NewQueryFrame(fields []string) (qf *QueryFrame, err error) {
	// "query_id","create_time","wall_time_millis","output_rows","written_output_rows","catalog","schema","session_properties","query"
	qf = &QueryFrame{
		QueryId:           fields[0],
		Catalog:           fields[5],
		Schema:            fields[6],
		SessionProperties: fields[7],
		Query:             strings.ReplaceAll(fields[8], "<<>>", "\n"),
	}
	qf.CreateTime, err = time.Parse(CreateTimeFormat, fields[1])
	if err == nil {
		qf.WallTimeMillis, err = strconv.Atoi(fields[2])
	}
	if err == nil {
		qf.OutputRows, err = strconv.Atoi(fields[3])
	}
	if err == nil {
		qf.WrittenOutputRows, err = strconv.Atoi(fields[4])
	}
	return qf, err
}

func (qf *QueryFrame) ParseSessionParams() map[string]any {
	m := make(map[string]any)
	if len(qf.SessionProperties) <= 2 || qf.SessionProperties[0] != '{' || qf.SessionProperties[len(qf.SessionProperties)-1] != '}' {
		return m
	}
	props := strings.Split(qf.SessionProperties[1:len(qf.SessionProperties)-1], ", ")
	for _, prop := range props {
		kv := strings.SplitN(prop, "=", 2)
		if len(kv) != 2 {
			log.Error().Str("prop", prop).Msg("invalid session property format")
			continue
		}
		m[kv[0]] = kv[1]
	}
	return m
}
