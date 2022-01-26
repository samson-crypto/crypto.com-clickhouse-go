package clickhouse_test

import (
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	sdecimal "github.com/shopspring/decimal"
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go"
	"github.com/stretchr/testify/assert"
)

func Test_Decimal128(t *testing.T) {
	const (
		ddl = `
			CREATE TABLE clickhouse_test_decimal128 (
				decimal  Decimal(38,0),
				decimalNullable  Nullable(Decimal(38,0))
			) Engine=Memory;
		`
		dml = `
			INSERT INTO clickhouse_test_decimal128 (
				decimal,
				decimalNullable
			) VALUES (
				?,
				?
			)
		`
		query = `
			SELECT
				decimal,
				decimalNullable
			FROM clickhouse_test_decimal128
		`
	)

	var (
		zero        int64 = 0
		negativeOne int64 = -1
		minInt64    int64 = math.MinInt64
		maxInt64    int64 = math.MaxInt64
	)

	minDecimal128 := "-99999999999999999999999999999999999999"
	maxDecimal128 := "99999999999999999999999999999999999999"

	if connect, err := clickhouse.OpenDirect("tcp://127.0.0.1:9000?debug=true"); assert.NoError(t, err) {
		{
			connect.Begin()
			stmt, _ := connect.Prepare("DROP TABLE IF EXISTS clickhouse_test_decimal128")
			stmt.Exec([]driver.Value{})
			connect.Commit()
		}
		{
			if _, err := connect.Begin(); assert.NoError(t, err) {
				if stmt, err := connect.Prepare(ddl); assert.NoError(t, err) {
					if _, err := stmt.Exec([]driver.Value{}); assert.NoError(t, err) {
						assert.NoError(t, connect.Commit())
					}
				}
			}
		}
		{
			if _, err := connect.Begin(); assert.NoError(t, err) {
				if _, err := connect.Prepare(dml); assert.NoError(t, err) {
					block, err := connect.Block()
					assert.NoError(t, err)

					// 1.
					err = block.AppendRow([]driver.Value{zero, nil})
					assert.NoError(t, err)

					// 2.
					err = block.AppendRow([]driver.Value{negativeOne, &zero})
					assert.NoError(t, err)

					// 3.
					err = block.AppendRow([]driver.Value{minInt64, &minInt64})
					assert.NoError(t, err)

					// 4.
					err = block.AppendRow([]driver.Value{maxInt64, &maxInt64})
					assert.NoError(t, err)

					// 5.
					err = block.AppendRow([]driver.Value{minDecimal128, &minDecimal128})
					assert.NoError(t, err)

					// 6.
					err = block.AppendRow([]driver.Value{maxDecimal128, &maxDecimal128})
					assert.NoError(t, err)

					assert.NoError(t, connect.Commit())
				}
			}
		}
	}
	if connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true"); assert.NoError(t, err) {
		if rows, err := connect.Query(query); assert.NoError(t, err) {
			assert.NoError(t, err)
			i := 0
			for rows.Next() {
				i++
				var decimal string
				var decimalNullable *string = nil

				switch i {
				case 1:
					if err := rows.Scan(
						&decimal,
						&decimalNullable,
					); assert.NoError(t, err) {
						assert.Equal(t, strconv.FormatInt(zero, 10), decimal)
						assert.Nil(t, decimalNullable)
					}

				case 2:
					if err := rows.Scan(
						&decimal,
						&decimalNullable,
					); assert.NoError(t, err) {
						assert.Equal(t, strconv.FormatInt(negativeOne, 10), decimal)
						assert.Equal(t, strconv.FormatInt(zero, 10), *decimalNullable)
					}

				case 3:
					if err := rows.Scan(
						&decimal,
						&decimalNullable,
					); assert.NoError(t, err) {
						assert.Equal(t, strconv.FormatInt(minInt64, 10), decimal)
						assert.Equal(t, strconv.FormatInt(minInt64, 10), *decimalNullable)
					}

				case 4:
					if err := rows.Scan(
						&decimal,
						&decimalNullable,
					); assert.NoError(t, err) {
						assert.Equal(t, strconv.FormatInt(maxInt64, 10), decimal)
						assert.Equal(t, strconv.FormatInt(maxInt64, 10), *decimalNullable)
					}

				case 5:
					if err := rows.Scan(
						&decimal,
						&decimalNullable,
					); assert.NoError(t, err) {
						assert.Equal(t, minDecimal128, decimal)
						assert.Equal(t, minDecimal128, *decimalNullable)
					}

				case 6:
					if err := rows.Scan(
						&decimal,
						&decimalNullable,
					); assert.NoError(t, err) {
						assert.Equal(t, maxDecimal128, decimal)
						assert.Equal(t, maxDecimal128, *decimalNullable)
					}
				}
			}
		}
	}
}

// -99999999999999999999999999999999999999
func getMinDecimal128Bytes() []byte {
	return []byte{
		0x01,
		0x00,
		0x00,
		0x00,
		0xc0,
		0xdd,
		0x75,
		0xf6,
		0x85,
		0x3b,
		0x79,
		0xa5,
		0x57,
		0xb3,
		0xc4,
		0xb4,
	}
}

// 99999999999999999999999999999999999999
func getMaxDecimal128Bytes() []byte {
	return []byte{
		0xff,
		0xff,
		0xff,
		0xff,
		0x3f,
		0x22,
		0x8a,
		0x09,
		0x7a,
		0xc4,
		0x86,
		0x5a,
		0xa8,
		0x4c,
		0x3b,
		0x4b,
	}
}

func int64ToDecimal128Bytes(v int64) []byte {
	bytes := make([]byte, 16)
	sign := 0
	if v < 0 {
		sign = -1
	}
	binary.LittleEndian.PutUint64(bytes[:8], uint64(v))
	binary.LittleEndian.PutUint64(bytes[8:], uint64(sign))
	return bytes
}

type TestDecimal struct {
	sdecimal.Decimal
}

func (d *TestDecimal) Scan(value interface{}) error {
	if v, ok := value.(sdecimal.Decimal); ok {
		d.Decimal = v
		return nil
	}
	return d.Decimal.Scan(value)
}

func Test_ParseDecimal128(t *testing.T) {
	const (
		ddl = `
			CREATE TABLE clickhouse_test_parse_decimal128 (
				decimal  Decimal(38,0),
				decimalNullable  Nullable(Decimal(38,0))
			) Engine=Memory;
		`
		dml = `
			INSERT INTO clickhouse_test_parse_decimal128 (
				decimal,
				decimalNullable
			) VALUES (
				?,
				?
			)
		`
		query = `
			SELECT
				decimal,
				decimalNullable
			FROM clickhouse_test_parse_decimal128
		`
	)

	var (
		zero        = sdecimal.Zero
		negativeOne = sdecimal.NewFromInt(-1)
		minInt64    = sdecimal.NewFromInt(math.MinInt64)
		maxInt64    = sdecimal.NewFromInt(math.MaxInt64)
	)

	minDecimal128 := getMinDecimal128Bytes()
	maxDecimal128 := getMaxDecimal128Bytes()

	if connect, err := clickhouse.OpenDirect("tcp://127.0.0.1:9000?debug=true&parse_decimal=true"); assert.NoError(t, err) {
		{
			connect.Begin()
			stmt, _ := connect.Prepare("DROP TABLE IF EXISTS clickhouse_test_parse_decimal128")
			stmt.Exec([]driver.Value{})
			connect.Commit()
		}
		{
			if _, err := connect.Begin(); assert.NoError(t, err) {
				if stmt, err := connect.Prepare(ddl); assert.NoError(t, err) {
					if _, err := stmt.Exec([]driver.Value{}); assert.NoError(t, err) {
						assert.NoError(t, connect.Commit())
					}
				}
			}
		}
		{
			if _, err := connect.Begin(); assert.NoError(t, err) {
				if _, err := connect.Prepare(dml); assert.NoError(t, err) {
					block, err := connect.Block()
					assert.NoError(t, err)

					// 1.
					err = block.AppendRow([]driver.Value{zero, nil})
					assert.NoError(t, err)

					// 2.
					err = block.AppendRow([]driver.Value{negativeOne, &zero})
					assert.NoError(t, err)

					// 3.
					err = block.AppendRow([]driver.Value{minInt64, &minInt64})
					assert.NoError(t, err)

					// 4.
					err = block.AppendRow([]driver.Value{maxInt64, &maxInt64})
					assert.NoError(t, err)

					// 5.
					err = block.AppendRow([]driver.Value{minDecimal128, &minDecimal128})
					assert.NoError(t, err)

					// 6.
					err = block.AppendRow([]driver.Value{maxDecimal128, &maxDecimal128})
					assert.NoError(t, err)

					assert.NoError(t, connect.Commit())
				}
			}
		}
	}
	if connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true&parse_decimal=true"); assert.NoError(t, err) {
		if rows, err := connect.Query(query); assert.NoError(t, err) {
			assert.NoError(t, err)
			i := 0
			for rows.Next() {
				i++
				var decimal TestDecimal
				var decimalNullable *TestDecimal = nil

				switch i {
				case 1:
					if err := rows.Scan(
						&decimal,
						&decimalNullable,
					); assert.NoError(t, err) {
						assert.Equal(t, zero.String(), decimal.String())
						assert.Nil(t, decimalNullable)
					}

				case 2:
					if err := rows.Scan(
						&decimal,
						&decimalNullable,
					); assert.NoError(t, err) {
						assert.Equal(t, negativeOne.String(), decimal.String())
						assert.Equal(t, zero.String(), decimalNullable.String())
					}

				case 3:
					if err := rows.Scan(
						&decimal,
						&decimalNullable,
					); assert.NoError(t, err) {
						assert.Equal(t, minInt64.String(), decimal.String())
						assert.Equal(t, minInt64.String(), decimalNullable.String())
					}

				case 4:
					if err := rows.Scan(
						&decimal,
						&decimalNullable,
					); assert.NoError(t, err) {
						assert.Equal(t, maxInt64.String(), decimal.String())
						assert.Equal(t, maxInt64.String(), decimalNullable.String())
					}

				case 5:
					if err := rows.Scan(
						&decimal,
						&decimalNullable,
					); assert.NoError(t, err) {
						assert.Equal(t, "-"+strings.Repeat("9", 38), decimal.String())
						assert.Equal(t, "-"+strings.Repeat("9", 38), decimalNullable.String())
					}

				case 6:
					if err := rows.Scan(
						&decimal,
						&decimalNullable,
					); assert.NoError(t, err) {
						assert.Equal(t, strings.Repeat("9", 38), decimal.String())
						assert.Equal(t, strings.Repeat("9", 38), decimalNullable.String())
					}
				}
			}
		}
	}
}