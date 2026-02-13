package replay

import (
	"encoding/csv"
	presto "github.com/ethanyzhang/presto-go"
	"github.com/stretchr/testify/assert"
	"io"
	"sort"
	"strings"
	"testing"
)

func TestFrame(t *testing.T) {
	testFile := `"query_id","create_time","wallTimeMillis","output_rows","written_output_rows","catalog","schema","session_properties","query"
"20240415_112042_61088_qa5fd","2024-04-15 11:20:42.755 UTC","99993","14","0","glue","ng_public","{query_max_scan_raw_input_bytes=500GB, iceberg.hive_statistics_merge_strategy=NUMBER_OF_DISTINCT_VALUES,TOTAL_SIZE_IN_BYTES, pushdown_subfields_enabled=true}","-- Looker Query Context '{""user_id"":2337,""history_slug"":""1d0df5dc263357e96a96310626454f6e"",""instance_slug"":""c59fc17fc46e0aeaf86d35ed33635ddc""}'  SELECT       (DATE_FORMAT(partner_data.created_date_local , '%Y-%m-%d'))             AS ""partner_data.dynamic_period"",         (MOD((DAY_OF_WEEK(partner_data.created_date_local ) % 7) - 1 + 7, 7)) AS ""partner_data.period_day_of_week_index"",         (DATE_FORMAT(partner_data.created_date_local ,'%W')) AS ""partner_data.period_day_of_week"",     COALESCE(SUM(partner_data.finished_rides ), 0) AS ""partner_data.finished_orders"" FROM ng_public.etl_partner_data  AS partner_data INNER JOIN admin_system_city  AS admin_system_city ON admin_system_city.id = partner_data.city_id INNER JOIN lks.LR_SHOSW1713178850318_admin_system_country AS admin_system_country ON admin_system_country.code = admin_system_city.country_code  WHERE ((( partner_data.created_date_local  ) >= ((DATE_ADD('day', -14, CAST(CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP)))) AND ( partner_data.created_date_local  ) < ((DATE_ADD('day', 14, DATE_ADD('day', -14, CAST(CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP))))))) AND (admin_system_city.name ) = 'Budapest' AND (admin_system_country.name ) = 'Hungary' GROUP BY     1,     2,     3 ORDER BY     1 DESC LIMIT 500"`
	stringReader := strings.NewReader(testFile)
	csvReader := csv.NewReader(stringReader)
	_, _ = csvReader.Read()
	for i := 1; ; i++ {
		fields, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		assert.LessOrEqual(t, i, 1)
		assert.Nil(t, err)
		frame, err := NewQueryFrame(fields)
		assert.Nil(t, err)
		assert.Equal(t, "20240415_112042_61088_qa5fd", frame.QueryId)
		assert.Equal(t, "2024-04-15 11:20:42.755 UTC", frame.CreateTime.Format(CreateTimeFormat))
		assert.Equal(t, 99993, frame.WallTimeMillis)
		client, _ := presto.NewClient("http://127.0.0.1")
		sessionParams := strings.Split(client.GenerateSessionParamsHeaderValue(frame.ParseSessionParams()), ",")
		sort.Strings(sessionParams)
		assert.Equal(t, []string{
			"iceberg.hive_statistics_merge_strategy=NUMBER_OF_DISTINCT_VALUES%2CTOTAL_SIZE_IN_BYTES",
			"pushdown_subfields_enabled=true",
			"query_max_scan_raw_input_bytes=500GB"}, sessionParams)
	}
}
