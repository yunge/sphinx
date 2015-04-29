package sphinx

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"io"
	"reflect"
	"strconv"
	"strings"
)

const (
	DefaultPK = "Id"
)

func (sc *Client) SetIndex(index string) *Client {
	if index == "" {
		sc.err = errors.New("SetIndex > Index name is empty!")
		return sc
	}

	sc.Index = index
	return sc
}

func (sc *Client) SetColumns(columns ...string) *Client {
	if len(columns) == 0 {
		sc.err = errors.New("SetColumns > Columns is empty!")
		return sc
	}

	sc.Columns = columns
	return sc
}

func (sc *Client) SetWhere(where string) *Client {
	if where == "" {
		sc.err = errors.New("SetWhere > where is empty!")
		return sc
	}

	sc.Where = where
	return sc
}

func (sc *Client) GetDb() (err error) {
	var addr string
	if sc.SqlSocket != "" {
		addr = "unix(" + sc.SqlSocket + ")"
	} else {
		// Already get default host and port in NewSQLClient()
		addr = "tcp(" + sc.Host + ":" + strconv.Itoa(sc.SqlPort) + ")"
	}

	if sc.DB, err = sql.Open("mysql", addr+"/"); err != nil {
		return err
	}
	
	// FIXME
	// The returned DB is safe for concurrent use by multiple goroutines and maintains its own pool of idle connections. 
	//sc.DB.SetMaxOpenConns(100)
	sc.DB.SetMaxIdleConns(10)

	return
}

// Caller should close db.
func (sc *Client) Init(obj interface{}) (err error) {
	// Init sql.DB
	//if sc.DB == nil {
	//	if err = sc.GetDb(); err != nil {
	//		return fmt.Errorf("Init > %v", err)
	//	}
	//}

	// Get object's reflect.Value
	if obj != nil { //some functions do not need sc.val
		sc.val = reflect.Indirect(reflect.ValueOf(obj))

		// check whether allValue is addressable, 'obj' must be a pointer!
		if !sc.val.CanAddr() {
			return fmt.Errorf("sc.Init> The obj value is unaddressable value")
		}
	}

	return
}

func (sc *Client) Execute(sqlStr string) (result sql.Result, err error) {
	// Init sql.DB
	if sc.DB == nil {
		if err = sc.GetDb(); err != nil {
			return nil, fmt.Errorf("Execute> %v", err)
		}
	}
	// It is rare to Close a DB, as the DB handle is meant to be
        // long-lived and shared between many goroutines.
	//defer sc.DB.Close()
	return sc.DB.Exec(sqlStr)
}

func (sc *Client) ExecuteReturnRowsAffected(sqlStr string) (rowsAffected int, err error) {
	result, err := sc.Execute(sqlStr)
	if err != nil {
		return -1, err
	}
	if result == nil {
		return -1, fmt.Errorf("ExecuteReturnRowsAffected: Nil result")
	}

	rowsAffected64, err := result.RowsAffected()
	if err != nil {
		return -1, fmt.Errorf("ExecuteReturnRowsAffected: %v", err)
	}

	rowsAffected = int(rowsAffected64)
	if rowsAffected < 0 {
		return rowsAffected, fmt.Errorf("ExecuteReturnRowsAffected> Nagtive RowsAffected(): %d", rowsAffected)
	}
	return
}

// Sphinx doesn't support LastInsertId now.
func (sc *Client) insert(obj interface{}, doReplace bool) (err error) {
	if err = sc.Init(obj); err != nil {
		return fmt.Errorf("Insert > %v", err)
	}

	var colVals []string
	// If not set sc.Columns, then use all fields as columns
	if len(sc.Columns) == 0 {
		if sc.val.Kind() == reflect.Struct {
			var appendField func(*[]string, *[]string, reflect.Value) error
			appendField = func(strs *[]string, vals *[]string, val reflect.Value) (err error) {
				for i := 0; i < val.NumField(); i++ {
					fieldVal := val.Field(i)
					sf := val.Type().Field(i)

					switch fieldVal.Type().Kind() {
					case reflect.Struct:
						if err = appendField(strs, vals, fieldVal); err != nil {
							return err
						}
					case reflect.Slice, reflect.Map:
						// just pass
					default:
						*strs = append(*strs, sf.Name)
						s, err := GetValQuoteStr(fieldVal)
						if err != nil {
							return err
						}
						*vals = append(*vals, s)
					}
				}

				return nil
			}

			if err = appendField(&sc.Columns, &colVals, sc.val); err != nil {
				return
			}
		} else {
			// if not struct，then it must just one ‘id’ field, "ID column must be present in all cases."
			sc.Columns = []string{DefaultPK}
			s, err := GetValQuoteStr(sc.val)
			if err != nil {
				return fmt.Errorf("Insert > %v", err)
			}
			colVals = []string{s}
		}

	} else if colVals, err = GetColVals(sc.val, sc.Columns); err != nil {
		return
	}

	var sqlStr string
	if doReplace {
		sqlStr = "REPLACE"
	} else {
		sqlStr = "INSERT"
	}
	sqlStr += fmt.Sprintf(" INTO %s (%s) VALUES (%s)", sc.Index, strings.Join(sc.Columns, ","), strings.Join(colVals, ","))

	//fmt.Printf("Insert sql: %s\n", sqlStr)
	if _, err = sc.Execute(sqlStr); err != nil {
		return fmt.Errorf("Insert > %v", err)
	}

	return
}

func (sc *Client) Insert(obj interface{}) error {
	// false means NOT do REPLACE
	return sc.insert(obj, false)
}

func (sc *Client) Replace(obj interface{}) error {
	// true means DO REPLACE
	return sc.insert(obj, true)
}

// Must set columns!
func (sc *Client) Update(obj interface{}) (rowsAffected int, err error) {
	if err = sc.Init(obj); err != nil {
		return -1, fmt.Errorf("Update > %v", err)
	}
	// Must set 'Columns'
	if len(sc.Columns) == 0 {
		return -1, fmt.Errorf("Update > columns is not set!")
	}

	colVals, err := GetColVals(sc.val, sc.Columns)
	if err != nil {
		return -1, fmt.Errorf("Update > %v", err)
	}

	var updateStr string
	for i, col := range sc.Columns {
		if colVals[i][0] == '\'' {
			return -1, fmt.Errorf("Update > Do not support update string field: %v", colVals)
		}
		updateStr += col + "=" + colVals[i] + ","
	}
	updateStr = updateStr[:len(updateStr)-1]

	// If not set "where", then set WHERE clause to "id=..."
	if sc.Where == "" {
		if sc.val.Kind() != reflect.Struct {
			return -1, fmt.Errorf("Update > If not set WHERE clause, then must be a struct object with Id field: %v", obj)
		}
		idVal := sc.val.FieldByName(DefaultPK)
		if idVal.Kind() != reflect.Int && !idVal.IsValid() {
			return -1, fmt.Errorf("Update > Invalid Id field: %v", obj)
		}

		sc.Where = DefaultPK + "=" + strconv.Itoa(int(idVal.Int()))
	}

	sqlStr := fmt.Sprintf("UPDATE %s SET %s WHERE %s", sc.Index, updateStr, sc.Where)
	//fmt.Printf("Update sql: %s\n", sqlStr)

	rowsAffected, err = sc.ExecuteReturnRowsAffected(sqlStr)
	if err != nil {
		return -1, fmt.Errorf("Update> %v\n", err)
	}

	return
}

// Must based on ID now.
func (sc *Client) Delete(obj interface{}) (rowsAffected int, err error) {
	if err = sc.Init(nil); err != nil {
		return -1, fmt.Errorf("Delete> %v", err)
	}

	sqlStr := "DELETE FROM " + sc.Index + " WHERE id "
	switch v := obj.(type) {
	case int:
		if v <= 0 {
			return -1, fmt.Errorf("Delete> Invalid id val: %d", v)
		}
		sqlStr += "= " + strconv.Itoa(v)
	case []int:
		if len(v) == 0 {
			return -1, fmt.Errorf("Delete> Empty []int")
		}

		sqlStr += "IN ("
		for _, id := range v {
			if id <= 0 {
				return -1, fmt.Errorf("Delete> Invalid id val: %d", id)
			}
			sqlStr += strconv.Itoa(id) + ","
		}
		sqlStr = sqlStr[:len(sqlStr)-1] + ")" // Change the last "," to ")"
	default:
		return -1, fmt.Errorf("Delete> Invalid type, must be int or []int: %#v", obj)
	}

	rowsAffected, err = sc.ExecuteReturnRowsAffected(sqlStr)
	if err != nil {
		return 0, fmt.Errorf("Delete>  %v", err)
	}
	return
}

// ATTACH currently supports empty target RT indexes only.
func (sc *Client) AttachToRT(diskIndex, rtIndex string) error {
	if diskIndex == "" || rtIndex == "" {
		return fmt.Errorf("AttachToRT > Empty index name. disk: '%s'  rt: '%s'", diskIndex, rtIndex)
	}

	if _, err := sc.Execute("ATTACH INDEX " + diskIndex + " TO RTINDEX " + rtIndex); err != nil {
		return fmt.Errorf("AttachToRT(%s) > %v", rtIndex, err)
	}
	return nil
}

// Forcibly flushes RT index RAM chunk contents to disk.
func (sc *Client) FlushRT(rtIndex string) error {
	if rtIndex == "" {
		return fmt.Errorf("FlushRT > Empty RT index name!")
	}

	if _, err := sc.Execute("FLUSH RTINDEX " + rtIndex); err != nil {
		return fmt.Errorf("FlushRT(%s) > %v", rtIndex, err)
	}
	return nil
}

// Added in 2.1.1-beta, clears the RT index completely.
func (sc *Client) TruncateRT(rtIndex string) error {
	if rtIndex == "" {
		return errors.New("TruncateRT > Empty RT index name!")
	}
	if _, err := sc.Execute("TRUNCATE RTINDEX " + rtIndex); err != nil {
		return fmt.Errorf("TruncateRT(%s) > %v", rtIndex, err)
	}
	return nil
}

// Added in 2.1.1-beta, enqueues a RT index for optimization in a background thread.
func (sc *Client) Optimize(rtIndex string) error {
	if rtIndex == "" {
		return errors.New("Optimize > Empty RT index name!")
	}
	if _, err := sc.Execute("OPTIMIZE INDEX " + rtIndex); err != nil {
		return fmt.Errorf("Optimize(%s) > %v", rtIndex, err)
	}
	return nil
}

/// Util funcs

func GetColVals(val reflect.Value, cols []string) (values []string, err error) {
	typ := val.Type()
	// if not struct, then must just have one column.
	if val.Kind() != reflect.Struct && len(cols) != 1 {
		return nil, fmt.Errorf("GetColVals> If not a struct(%s), must have one column: %v", val.Kind(), cols)
	}

	values = make([]string, len(cols))
	for i, col := range cols {
		var fieldVal reflect.Value
		if val.Kind() == reflect.Struct {
			fieldIndex := getFieldIndexByName(typ, col)
			if fieldIndex[0] < 0 {
				return nil, fmt.Errorf("GetColVals> Can't found struct field(column): '%s'\n", col)
			}
			fieldVal = val.FieldByIndex(fieldIndex)
		} else {
			fieldVal = val
		}

		if values[i], err = GetValQuoteStr(fieldVal); err != nil {
			return
		}
	}

	return
}

// for insert and update
// If already assigned, then just ignore tag
func GetValQuoteStr(val reflect.Value) (string, error) {
	switch val.Kind() {
	case reflect.Bool:
		boolStr := "N"
		if val.Bool() {
			boolStr = "Y"
		}
		return boolStr, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(val.Int(), 10), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return strconv.FormatUint(val.Uint(), 10), nil
	case reflect.Float32, reflect.Float64:
		return strconv.FormatFloat(val.Float(), 'f', -1, 64), nil
	case reflect.String:
		return QuoteStr(val.String()), nil
	case reflect.Slice: //[]byte
		if val.Type().Elem().Name() != "uint8" {
			return "", fmt.Errorf("GetValQuoteStr> slicetype is not []byte: %v", val.Interface())
		}
		return QuoteStr(string(val.Interface().([]byte))), nil
	default:
		return "", fmt.Errorf("GetValQuoteStr> reflect.Value is not a string/int/uint/float/bool/[]byte!\nval: %v", val)
	}
	return "", nil
}

func getFieldIndexByName(typ reflect.Type, name string) (index []int) {
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		var subIndex []int
		if field.Type.Kind() == reflect.Struct {
			if subIndex = getFieldIndexByName(field.Type, name); subIndex[0] >= 0 {
				return append([]int{i}, subIndex...)
			}
		}

		if field.Name == name {
			return []int{i}
		}
	}
	return []int{-1}
}

func QuoteStr(s string) string {
	return "'" + escapeString(s) + "'"
}

// mysql_real_escape_string()  “\”, “'”, “"”, NUL (ASCII 0), “\n”, “\r”, and Control+Z
func escapeString(txt string) string {
	var (
		esc string
		buf bytes.Buffer
	)
	last := 0
	for ii, bb := range txt {
		switch bb {
		case 0:
			esc = `\0`
		case '\n':
			esc = `\n`
		case '\r':
			esc = `\r`
		case '\\':
			esc = `\\`
		case '\'':
			esc = `\'`
		case '"':
			esc = `\"`
		case '\032':
			esc = `\Z`
		default:
			continue
		}
		io.WriteString(&buf, txt[last:ii])
		io.WriteString(&buf, esc)
		last = ii + 1
	}
	io.WriteString(&buf, txt[last:])
	return buf.String()
}
