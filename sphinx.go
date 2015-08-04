package sphinx

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"reflect"
	"strings"
	"time"
)

/* searchd command versions */
const (
	VER_MAJOR_PROTO        = 0x1
	VER_COMMAND_SEARCH     = 0x119 // 0x11D for 2.1
	VER_COMMAND_EXCERPT    = 0x104
	VER_COMMAND_UPDATE     = 0x102 // 0x103 for 2.1
	VER_COMMAND_KEYWORDS   = 0x100
	VER_COMMAND_STATUS     = 0x100
	VER_COMMAND_FLUSHATTRS = 0x100
)

/* matching modes */
const (
	SPH_MATCH_ALL = iota
	SPH_MATCH_ANY
	SPH_MATCH_PHRASE
	SPH_MATCH_BOOLEAN
	SPH_MATCH_EXTENDED
	SPH_MATCH_FULLSCAN
	SPH_MATCH_EXTENDED2
)

/* ranking modes (extended2 only) */
const (
	SPH_RANK_PROXIMITY_BM25 = iota // Default mode, phrase proximity major factor and BM25 minor one
	SPH_RANK_BM25
	SPH_RANK_NONE
	SPH_RANK_WORDCOUNT
	SPH_RANK_PROXIMITY
	SPH_RANK_MATCHANY
	SPH_RANK_FIELDMASK
	SPH_RANK_SPH04
	SPH_RANK_EXPR
	SPH_RANK_TOTAL
)

/* sorting modes */
const (
	SPH_SORT_RELEVANCE = iota
	SPH_SORT_ATTR_DESC
	SPH_SORT_ATTR_ASC
	SPH_SORT_TIME_SEGMENTS
	SPH_SORT_EXTENDED
	SPH_SORT_EXPR // Deprecated, never use it.
)

/* grouping functions */
const (
	SPH_GROUPBY_DAY = iota
	SPH_GROUPBY_WEEK
	SPH_GROUPBY_MONTH
	SPH_GROUPBY_YEAR
	SPH_GROUPBY_ATTR
	SPH_GROUPBY_ATTRPAIR
)

/* searchd reply status codes */
const (
	SEARCHD_OK = iota
	SEARCHD_ERROR
	SEARCHD_RETRY
	SEARCHD_WARNING
)

/* attribute types */
const (
	SPH_ATTR_NONE = iota
	SPH_ATTR_INTEGER
	SPH_ATTR_TIMESTAMP
	SPH_ATTR_ORDINAL
	SPH_ATTR_BOOL
	SPH_ATTR_FLOAT
	SPH_ATTR_BIGINT
	SPH_ATTR_STRING
	SPH_ATTR_MULTI   = 0x40000001
	SPH_ATTR_MULTI64 = 0x40000002
)

/* searchd commands */
const (
	SEARCHD_COMMAND_SEARCH = iota
	SEARCHD_COMMAND_EXCERPT
	SEARCHD_COMMAND_UPDATE
	SEARCHD_COMMAND_KEYWORDS
	SEARCHD_COMMAND_PERSIST
	SEARCHD_COMMAND_STATUS
	SEARCHD_COMMAND_QUERY
	SEARCHD_COMMAND_FLUSHATTRS
)

/* filter types */
const (
	SPH_FILTER_VALUES = iota
	SPH_FILTER_RANGE
	SPH_FILTER_FLOATRANGE
)

type filter struct {
	attr       string
	filterType int
	values     []uint64
	umin       uint64
	umax       uint64
	fmin       float32
	fmax       float32
	exclude    bool
}

type override struct {
	attrName string
	attrType int
	values   map[uint64]interface{}
}

type Match struct {
	DocId      uint64        // Matched document ID.
	Weight     int           // Matched document weight.
	AttrValues []interface{} // Matched document attribute values.
}

type WordInfo struct {
	Word string // Word form as returned from search daemon, stemmed or otherwise postprocessed.
	Docs int    // Total amount of matching documents in collection.
	Hits int    // Total amount of hits (occurences) in collection.
}

type Result struct {
	Fields     []string   // Full-text field namess.
	AttrNames  []string   // Attribute names.
	AttrTypes  []int      // Attribute types (refer to SPH_ATTR_xxx constants in Client).
	Matches    []Match    // Retrieved matches.
	Total      int        // Total matches in this result set.
	TotalFound int        // Total matches found in the index(es).
	Time       float32    // Elapsed time (as reported by searchd), in seconds.
	Words      []WordInfo // Per-word statistics.

	Warning string
	Error   error
	Status  int // Query status (refer to SEARCHD_xxx constants in Client).
}

type Options struct {
	Host          string
	Port          int
	Socket        string // Unix socket
	SqlPort       int
	SqlSocket     string
	RetryCount    int
	RetryDelay    int
	Timeout       int
	Offset        int // how many records to seek from result-set start
	Limit         int // how many records to return from result-set starting at offset (default is 20)
	MaxMatches    int // max matches to retrieve
	Cutoff        int // cutoff to stop searching at
	MaxQueryTime  int
	Select        string // select-list (attributes or expressions, with optional aliases)
	MatchMode     int    // query matching mode (default is SPH_MATCH_ALL)
	RankMode      int
	RankExpr      string // ranking expression for SPH_RANK_EXPR
	SortMode      int    // match sorting mode (default is SPH_SORT_RELEVANCE)
	SortBy        string // attribute to sort by (defualt is "")
	MinId         uint64 // min ID to match (default is 0, which means no limit)
	MaxId         uint64 // max ID to match (default is 0, which means no limit)
	LatitudeAttr  string
	LongitudeAttr string
	Latitude      float32
	Longitude     float32
	GroupBy       string // group-by attribute name
	GroupFunc     int    // group-by function (to pre-process group-by attribute value with)
	GroupSort     string // group-by sorting clause (to sort groups in result set with)
	GroupDistinct string // group-by count-distinct attribute

	// for sphinxql
	Index   string // index name for sphinxql query.
	Columns []string
	Where   string
}

type Client struct {
	*Options
	conn net.Conn

	warning   string
	err       error
	connerror bool // connection error vs remote error flag

	weights []int // per-field weights (default is 1 for all fields)
	filters []filter
	reqs    [][]byte // requests array for multi-query

	indexWeights map[string]int
	fieldWeights map[string]int
	overrides    map[string]override

	// For sphinxql
	DB  *sql.DB       // Capitalize, so that can "defer sc.Db.Close()"
	val reflect.Value // object parameter's reflect value
}

// You can change it, so that you do not need to call Set***() every time.
var DefaultOptions = &Options{
	Host:       "localhost",
	Port:       9312,
	SqlPort:    9306,
	Limit:      20,
	MatchMode:  SPH_MATCH_EXTENDED, // "When you use one of the legacy modes, Sphinx internally converts the query to the appropriate new syntax and chooses the appropriate ranker."
	SortMode:   SPH_SORT_RELEVANCE,
	GroupFunc:  SPH_GROUPBY_DAY,
	GroupSort:  "@group desc",
	MaxMatches: 1000,
	Timeout:    1000,
	RankMode:   SPH_RANK_PROXIMITY_BM25,
	Select:     "*",
}

func NewClient(opts ...*Options) (sc *Client) {
	if opts != nil {
		return &Client{Options: opts[0]}
	}
	return &Client{Options: DefaultOptions}
}

/***** General API functions *****/

func (sc *Client) GetLastError() error {
	return sc.err
}

// Just for convenience
func (sc *Client) Error() error {
	return sc.err
}

func (sc *Client) GetLastWarning() string {
	return sc.warning
}

// Note: this func also can set sc.Socket(unix socket).
// You can just use ""/0 as default value.
func (sc *Client) SetServer(host string, port int) *Client {
	isTcpMode := true

	if host != "" {
		if host[0] == '/' {
			sc.Socket = host
			isTcpMode = false
		} else if len(host) > 7 && host[:7] == "unix://" {
			sc.Socket = host[7:]
			isTcpMode = false
		} else {
			sc.Host = host
		}
	} else {
		sc.Host = DefaultOptions.Host
	}

	if isTcpMode {
		if port > 0 {
			sc.Port = port
		} else {
			sc.Port = DefaultOptions.Port
		}
	}

	return sc
}

func (sc *Client) SetSqlServer(host string, sqlport int) *Client {
	isTcpMode := true

	if host != "" {
		if host[0] == '/' {
			sc.SqlSocket = host
			isTcpMode = false
		} else if len(host) > 7 && host[:7] == "unix://" {
			sc.SqlSocket = host[7:]
			isTcpMode = false
		} else {
			sc.Host = host
		}
	} else {
		sc.Host = DefaultOptions.Host
	}

	if isTcpMode {
		if sqlport > 0 {
			sc.SqlPort = sqlport
		} else {
			sc.SqlPort = DefaultOptions.SqlPort
		}
	}

	return sc
}

func (sc *Client) SetRetries(count, delay int) *Client {
	if count < 0 {
		sc.err = fmt.Errorf("SetRetries > count must not be negative: %d", count)
		return sc
	}
	if delay < 0 {
		sc.err = fmt.Errorf("SetRetries > delay must not be negative: %d", delay)
		return sc
	}

	sc.RetryCount = count
	sc.RetryDelay = delay
	return sc
}

// millisecond, not nanosecond.
func (sc *Client) SetConnectTimeout(timeout int) *Client {
	if timeout < 0 {
		sc.err = fmt.Errorf("SetConnectTimeout > connect timeout must not be negative: %d", timeout)
		return sc
	}

	sc.Timeout = timeout
	return sc
}

func (sc *Client) IsConnectError() bool {
	return sc.connerror
}

/***** General query settings *****/

// Set matches offset and limit to return to client, max matches to retrieve on server, and cutoff.
func (sc *Client) SetLimits(offset, limit, maxMatches, cutoff int) *Client {
	if offset < 0 {
		sc.err = fmt.Errorf("SetLimits > offset must not be negative: %d", offset)
		return sc
	}
	if limit <= 0 {
		sc.err = fmt.Errorf("SetLimits > limit must be positive: %d", limit)
		return sc
	}
	if maxMatches <= 0 {
		sc.err = fmt.Errorf("SetLimits > maxMatches must be positive: %d", maxMatches)
		return sc
	}
	if cutoff < 0 {
		sc.err = fmt.Errorf("SetLimits > cutoff must not be negative: %d", cutoff)
		return sc
	}

	sc.Offset = offset
	sc.Limit = limit
	if maxMatches > 0 {
		sc.MaxMatches = maxMatches
	}
	if cutoff > 0 {
		sc.Cutoff = cutoff
	}
	return sc
}

// Set maximum query time, in milliseconds, per-index, 0 means "do not limit".
func (sc *Client) SetMaxQueryTime(maxQueryTime int) *Client {
	if maxQueryTime < 0 {
		sc.err = fmt.Errorf("SetMaxQueryTime > maxQueryTime must not be negative: %d", maxQueryTime)
		return sc
	}

	sc.MaxQueryTime = maxQueryTime
	return sc
}

func (sc *Client) SetOverride(attrName string, attrType int, values map[uint64]interface{}) *Client {
	if attrName == "" {
		sc.err = errors.New("SetOverride > attrName is empty!")
		return sc
	}
	// Min value is 'SPH_ATTR_INTEGER = 1', not '0'.
	if (attrType < 1 || attrType > SPH_ATTR_STRING) && attrType != SPH_ATTR_MULTI && SPH_ATTR_MULTI != SPH_ATTR_MULTI64 {
		sc.err = fmt.Errorf("SetOverride > invalid attrType: %d", attrType)
		return sc
	}

	sc.overrides[attrName] = override{
		attrName: attrName,
		attrType: attrType,
		values:   values,
	}
	return sc
}

func (sc *Client) SetSelect(s string) *Client {
	if s == "" {
		sc.err = errors.New("SetSelect > selectStr is empty!")
		return sc
	}

	sc.Select = s
	return sc
}

/***** Full-text search query settings *****/

func (sc *Client) SetMatchMode(mode int) *Client {
	if mode < 0 || mode > SPH_MATCH_EXTENDED2 {
		sc.err = fmt.Errorf("SetMatchMode > unknown mode value; use one of the SPH_MATCH_xxx constants: %d", mode)
		return sc
	}

	sc.MatchMode = mode
	return sc
}

func (sc *Client) SetRankingMode(ranker int, rankexpr ...string) *Client {
	if ranker < 0 || ranker > SPH_RANK_TOTAL {
		sc.err = fmt.Errorf("SetRankingMode > unknown ranker value; use one of the SPH_RANK_xxx constants: %d", ranker)
		return sc
	}

	sc.RankMode = ranker

	if len(rankexpr) > 0 {
		if ranker != SPH_RANK_EXPR {
			sc.err = fmt.Errorf("SetRankingMode > rankexpr must used with SPH_RANK_EXPR! ranker: %d  rankexpr: %s", ranker, rankexpr)
			return sc
		}

		sc.RankExpr = rankexpr[0]
	}

	return sc
}

func (sc *Client) SetSortMode(mode int, sortBy string) *Client {
	if mode < 0 || mode > SPH_SORT_EXPR {
		sc.err = fmt.Errorf("SetSortMode > unknown mode value; use one of the available SPH_SORT_xxx constants: %d", mode)
		return sc
	}
	/*SPH_SORT_RELEVANCE ignores any additional parameters and always sorts matches by relevance rank.
	All other modes require an additional sorting clause.*/
	if (mode != SPH_SORT_RELEVANCE) && (sortBy == "") {
		sc.err = fmt.Errorf("SetSortMode > sortby string must not be empty in selected mode: %d", mode)
		return sc
	}

	sc.SortMode = mode
	sc.SortBy = sortBy
	return sc
}

func (sc *Client) SetFieldWeights(weights map[string]int) *Client {
	// Default weight value is 1.
	for field, weight := range weights {
		if weight < 1 {
			sc.err = fmt.Errorf("SetFieldWeights > weights must be positive 32-bit integers, field:%s  weight:%d", field, weight)
			return sc
		}
	}

	sc.fieldWeights = weights
	return sc
}

func (sc *Client) SetIndexWeights(weights map[string]int) *Client {
	for field, weight := range weights {
		if weight < 1 {
			sc.err = fmt.Errorf("SetIndexWeights > weights must be positive 32-bit integers, field:%s  weight:%d", field, weight)
			return sc
		}
	}

	sc.indexWeights = weights
	return sc
}

/***** Result set filtering settings *****/

func (sc *Client) SetIDRange(min, max uint64) *Client {
	if min > max {
		sc.err = fmt.Errorf("SetIDRange > min > max! min:%d  max:%d", min, max)
		return sc
	}

	sc.MinId = min
	sc.MaxId = max
	return sc
}

func (sc *Client) SetFilter(attr string, values []uint64, exclude bool) *Client {
	if attr == "" {
		sc.err = fmt.Errorf("SetFilter > attribute name is empty!")
		return sc
	}
	if len(values) == 0 {
		sc.err = fmt.Errorf("SetFilter > values is empty!")
		return sc
	}

	sc.filters = append(sc.filters, filter{
		filterType: SPH_FILTER_VALUES,
		attr:       attr,
		values:     values,
		exclude:    exclude,
	})
	return sc
}

func (sc *Client) SetFilterRange(attr string, umin, umax uint64, exclude bool) *Client {
	if attr == "" {
		sc.err = fmt.Errorf("SetFilterRange > attribute name is empty!")
		return sc
	}
	if umin > umax {
		sc.err = fmt.Errorf("SetFilterRange > min > max! umin:%d  umax:%d", umin, umax)
		return sc
	}

	sc.filters = append(sc.filters, filter{
		filterType: SPH_FILTER_RANGE,
		attr:       attr,
		umin:       umin,
		umax:       umax,
		exclude:    exclude,
	})
	return sc
}

func (sc *Client) SetFilterFloatRange(attr string, fmin, fmax float32, exclude bool) *Client {
	if attr == "" {
		sc.err = fmt.Errorf("SetFilterFloatRange > attribute name is empty!")
		return sc
	}
	if fmin > fmax {
		sc.err = fmt.Errorf("SetFilterFloatRange > min > max! fmin:%d  fmax:%d", fmin, fmax)
		return sc
	}

	sc.filters = append(sc.filters, filter{
		filterType: SPH_FILTER_FLOATRANGE,
		attr:       attr,
		fmin:       fmin,
		fmax:       fmax,
		exclude:    exclude,
	})
	return sc
}

// The latitude and longitude are expected to be in radians. Use DegreeToRadian() to transform degree values.
func (sc *Client) SetGeoAnchor(latitudeAttr, longitudeAttr string, latitude, longitude float32) *Client {
	if latitudeAttr == "" {
		sc.err = fmt.Errorf("SetGeoAnchor > latitudeAttr is empty!")
		return sc
	}
	if longitudeAttr == "" {
		sc.err = fmt.Errorf("SetGeoAnchor > longitudeAttr is empty!")
		return sc
	}

	sc.LatitudeAttr = latitudeAttr
	sc.LongitudeAttr = longitudeAttr
	sc.Latitude = latitude
	sc.Longitude = longitude
	return sc
}

/***** GROUP BY settings *****/

func (sc *Client) SetGroupBy(groupBy string, groupFunc int, groupSort string) *Client {
	if groupFunc < 0 || groupFunc > SPH_GROUPBY_ATTRPAIR {
		sc.err = fmt.Errorf("SetGroupBy > unknown groupFunc value: '%d', use one of the available SPH_GROUPBY_xxx constants.", groupFunc)
		return sc
	}

	sc.GroupBy = groupBy
	sc.GroupFunc = groupFunc
	sc.GroupSort = groupSort
	return sc
}

func (sc *Client) SetGroupDistinct(groupDistinct string) *Client {
	if groupDistinct == "" {
		sc.err = errors.New("SetGroupDistinct > groupDistinct is empty!")
		return sc
	}
	sc.GroupDistinct = groupDistinct
	return sc
}

/***** Querying *****/

func (sc *Client) Query(query, index, comment string) (result *Result, err error) {
	if index == "" {
		index = "*"
	}

	// reset requests array
	sc.reqs = nil
	if _, err = sc.AddQuery(query, index, comment); err != nil {
		return nil, err
	}

	results, err := sc.RunQueries()
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, fmt.Errorf("Query > Empty results!\nClient: %#v", sc)
	}

	result = &results[0]
	if result.Error != nil {
		return nil, fmt.Errorf("Query > Result error: %v", result.Error)
	}

	sc.warning = result.Warning
	return
}

func (sc *Client) AddQuery(query, index, comment string) (i int, err error) {
	var req []byte

	req = writeInt32ToBytes(req, sc.Offset)
	req = writeInt32ToBytes(req, sc.Limit)
	req = writeInt32ToBytes(req, sc.MatchMode)
	req = writeInt32ToBytes(req, sc.RankMode)
	if sc.RankMode == SPH_RANK_EXPR {
		req = writeLenStrToBytes(req, sc.RankExpr)
	}
	req = writeInt32ToBytes(req, sc.SortMode)
	req = writeLenStrToBytes(req, sc.SortBy)
	req = writeLenStrToBytes(req, query)

	req = writeInt32ToBytes(req, len(sc.weights))
	for _, w := range sc.weights {
		req = writeInt32ToBytes(req, w)
	}

	req = writeLenStrToBytes(req, index)

	req = writeInt32ToBytes(req, 1) // id64 range marker
	req = writeInt64ToBytes(req, sc.MinId)
	req = writeInt64ToBytes(req, sc.MaxId)

	req = writeInt32ToBytes(req, len(sc.filters))
	for _, f := range sc.filters {
		req = writeLenStrToBytes(req, f.attr)
		req = writeInt32ToBytes(req, f.filterType)

		switch f.filterType {
		case SPH_FILTER_VALUES:
			req = writeInt32ToBytes(req, len(f.values))
			for _, v := range f.values {
				req = writeInt64ToBytes(req, v)
			}
		case SPH_FILTER_RANGE:
			req = writeInt64ToBytes(req, f.umin)
			req = writeInt64ToBytes(req, f.umax)
		case SPH_FILTER_FLOATRANGE:
			req = writeFloat32ToBytes(req, f.fmin)
			req = writeFloat32ToBytes(req, f.fmax)
		}

		if f.exclude {
			req = writeInt32ToBytes(req, 1)
		} else {
			req = writeInt32ToBytes(req, 0)
		}
	}

	req = writeInt32ToBytes(req, sc.GroupFunc)
	req = writeLenStrToBytes(req, sc.GroupBy)

	req = writeInt32ToBytes(req, sc.MaxMatches)
	req = writeLenStrToBytes(req, sc.GroupSort)

	req = writeInt32ToBytes(req, sc.Cutoff)
	req = writeInt32ToBytes(req, sc.RetryCount)
	req = writeInt32ToBytes(req, sc.RetryDelay)

	req = writeLenStrToBytes(req, sc.GroupDistinct)

	if sc.LatitudeAttr == "" || sc.LongitudeAttr == "" {
		req = writeInt32ToBytes(req, 0)
	} else {
		req = writeInt32ToBytes(req, 1)
		req = writeLenStrToBytes(req, sc.LatitudeAttr)
		req = writeLenStrToBytes(req, sc.LongitudeAttr)
		req = writeFloat32ToBytes(req, sc.Latitude)
		req = writeFloat32ToBytes(req, sc.Longitude)
	}

	req = writeInt32ToBytes(req, len(sc.indexWeights))
	for ind, wei := range sc.indexWeights {
		req = writeLenStrToBytes(req, ind)
		req = writeInt32ToBytes(req, wei)
	}

	req = writeInt32ToBytes(req, sc.MaxQueryTime)

	req = writeInt32ToBytes(req, len(sc.fieldWeights))
	for fie, wei := range sc.fieldWeights {
		req = writeLenStrToBytes(req, fie)
		req = writeInt32ToBytes(req, wei)
	}

	req = writeLenStrToBytes(req, comment)

	// attribute overrides
	req = writeInt32ToBytes(req, len(sc.overrides))
	for _, override := range sc.overrides {
		req = writeLenStrToBytes(req, override.attrName)
		req = writeInt32ToBytes(req, override.attrType)
		req = writeInt32ToBytes(req, len(override.values))
		for id, v := range override.values {
			req = writeInt64ToBytes(req, id)
			switch override.attrType {
			case SPH_ATTR_INTEGER:
				req = writeInt32ToBytes(req, v.(int))
			case SPH_ATTR_FLOAT:
				req = writeFloat32ToBytes(req, v.(float32))
			case SPH_ATTR_BIGINT:
				req = writeInt64ToBytes(req, v.(uint64))
			default:
				return -1, fmt.Errorf("AddQuery > attr value is not int/float32/uint64.")
			}
		}
	}

	// select-list
	req = writeLenStrToBytes(req, sc.Select)

	// send query, get response
	sc.reqs = append(sc.reqs, req)
	return len(sc.reqs) - 1, nil
}

//Returns None on network IO failure; or an array of result set hashes on success.
func (sc *Client) RunQueries() (results []Result, err error) {
	if len(sc.reqs) == 0 {
		return nil, fmt.Errorf("RunQueries > No queries defined, issue AddQuery() first.")
	}

	nreqs := len(sc.reqs)
	var allReqs []byte

	allReqs = writeInt32ToBytes(allReqs, 0) // it's a client
	allReqs = writeInt32ToBytes(allReqs, nreqs)
	for _, req := range sc.reqs {
		allReqs = append(allReqs, req...)
	}

	response, err := sc.doRequest(SEARCHD_COMMAND_SEARCH, VER_COMMAND_SEARCH, allReqs)
	if err != nil {
		return nil, err
	}

	var bp = byteParser{stream: response}

	for i := 0; i < nreqs; i++ {
		var result = Result{Status: -1} // Default value of status is 0, but SEARCHD_OK = 0, so must set it to another num.

		result.Status = bp.Int32()
		if result.Status != SEARCHD_OK {
			message := bp.String()

			if result.Status == SEARCHD_WARNING {
				result.Warning = string(message)
			} else {
				result.Error = errors.New(string(message))
				results = append(results, result)
				continue
			}
		}

		// read schema
		nfields := bp.Int32()
		result.Fields = make([]string, nfields)
		for fieldNum := 0; fieldNum < nfields; fieldNum++ {
			result.Fields[fieldNum] = bp.String()
		}

		nattrs := bp.Int32()
		result.AttrNames = make([]string, nattrs)
		result.AttrTypes = make([]int, nattrs)
		for attrNum := 0; attrNum < nattrs; attrNum++ {
			result.AttrNames[attrNum] = bp.String()
			result.AttrTypes[attrNum] = bp.Int32()
		}

		// read match count
		count := bp.Int32()
		id64 := bp.Int32() // if id64 == 1, then docId is uint64
		result.Matches = make([]Match, count)
		for matchesNum := 0; matchesNum < count; matchesNum++ {
			var match Match
			if id64 == 1 {
				match.DocId = bp.Uint64()
			} else {
				match.DocId = uint64(bp.Uint32())
			}
			match.Weight = bp.Int32()

			match.AttrValues = make([]interface{}, nattrs)

			for attrNum := 0; attrNum < len(result.AttrTypes); attrNum++ {
				attrType := result.AttrTypes[attrNum]
				switch attrType {
				case SPH_ATTR_BIGINT:
					match.AttrValues[attrNum] = bp.Uint64()
				case SPH_ATTR_FLOAT:
					f, err := bp.Float32()
					if err != nil {
						return nil, fmt.Errorf("binary.Read error: %v", err)
					}
					match.AttrValues[attrNum] = f
				case SPH_ATTR_STRING:
					match.AttrValues[attrNum] = bp.String()
				case SPH_ATTR_MULTI: // SPH_ATTR_MULTI is 2^30+1, not an int value.
					nvals := bp.Int32()
					var vals = make([]uint32, nvals)
					for valNum := 0; valNum < nvals; valNum++ {
						vals[valNum] = bp.Uint32()
					}
					match.AttrValues[attrNum] = vals
				case SPH_ATTR_MULTI64:
					nvals := bp.Int32()
					nvals = nvals / 2
					var vals = make([]uint64, nvals)
					for valNum := 0; valNum < nvals; valNum++ {
						vals[valNum] = uint64(bp.Uint32())
						bp.Uint32()
					}
					match.AttrValues[attrNum] = vals
				default: // handle everything else as unsigned ints
					match.AttrValues[attrNum] = bp.Uint32()
				}
			}
			result.Matches[matchesNum] = match
		}

		result.Total = bp.Int32()
		result.TotalFound = bp.Int32()

		msecs := bp.Uint32()
		result.Time = float32(msecs) / 1000.0

		nwords := bp.Int32()

		result.Words = make([]WordInfo, nwords)
		for wordNum := 0; wordNum < nwords; wordNum++ {
			result.Words[wordNum].Word = bp.String()
			result.Words[wordNum].Docs = bp.Int32()
			result.Words[wordNum].Hits = bp.Int32()
		}

		results = append(results, result)
	}
	sc.reqs = nil
	return
}

func (sc *Client) ResetFilters() {
	sc.filters = []filter{}

	/* reset GEO anchor */
	sc.LatitudeAttr = ""
	sc.LongitudeAttr = ""
	sc.Latitude = 0.0
	sc.Longitude = 0.0
}

func (sc *Client) ResetGroupBy() {
	sc.GroupBy = ""
	sc.GroupFunc = SPH_GROUPBY_DAY
	sc.GroupSort = "@group desc"
	sc.GroupDistinct = ""
}

/***** Additional functionality *****/

// all bool values are default false.
type ExcerptsOpts struct {
	BeforeMatch        string // default is "<b>".
	AfterMatch         string // default is "</b>".
	ChunkSeparator     string // A string to insert between snippet chunks (passages). Default is " ... ".
	Limit              int    // Maximum snippet size, in symbols (codepoints). default is 256.
	Around             int    // How much words to pick around each matching keywords block. default is 5.
	ExactPhrase        bool   // Whether to highlight exact query phrase matches only instead of individual keywords.
	SinglePassage      bool   // Whether to extract single best passage only.
	UseBoundaries      bool   // Whether to additionaly break passages by phrase boundary characters, as configured in index settings with phrase_boundary directive.
	WeightOrder        bool   // Whether to sort the extracted passages in order of relevance (decreasing weight), or in order of appearance in the document (increasing position).
	QueryMode          bool   // Whether to handle $words as a query in extended syntax, or as a bag of words (default behavior).
	ForceAllWords      bool   // Ignores the snippet length limit until it includes all the keywords.
	LimitPassages      int    // Limits the maximum number of passages that can be included into the snippet. default is 0 (no limit).
	LimitWords         int    // Limits the maximum number of keywords that can be included into the snippet. default is 0 (no limit).
	StartPassageId     int    // Specifies the starting value of %PASSAGE_ID% macro (that gets detected and expanded in BeforeMatch, AfterMatch strings). default is 1.
	LoadFiles          bool   // Whether to handle $docs as data to extract snippets from (default behavior), or to treat it as file names, and load data from specified files on the server side.
	LoadFilesScattered bool   // It assumes "load_files" option, and works only with distributed snippets generation with remote agents. The source files for snippets could be distributed among different agents, and the main daemon will merge together all non-erroneous results. So, if one agent of the distributed index has 'file1.txt', another has 'file2.txt' and you call for the snippets with both these files, the sphinx will merge results from the agents together, so you will get the snippets from both 'file1.txt' and 'file2.txt'.
	HtmlStripMode      string // HTML stripping mode setting. Defaults to "index", allowed values are "none", "strip", "index", and "retain".
	AllowEmpty         bool   // Allows empty string to be returned as highlighting result when a snippet could not be generated (no keywords match, or no passages fit the limit). By default, the beginning of original text would be returned instead of an empty string.
	PassageBoundary    string // Ensures that passages do not cross a sentence, paragraph, or zone boundary (when used with an index that has the respective indexing settings enabled). String, allowed values are "sentence", "paragraph", and "zone".
	EmitZones          bool   // Emits an HTML tag with an enclosing zone name before each passage.
}

func (sc *Client) BuildExcerpts(docs []string, index, words string, opts ExcerptsOpts) (resDocs []string, err error) {
	if len(docs) == 0 {
		return nil, errors.New("BuildExcerpts > Have no documents to process!")
	}
	if index == "" {
		return nil, errors.New("BuildExcerpts > index name is empty!")
	}
	if words == "" {
		return nil, errors.New("BuildExcerpts > Have no words to highlight!")
	}
	if opts.PassageBoundary != "" && opts.PassageBoundary != "sentence" && opts.PassageBoundary != "paragraph" && opts.PassageBoundary != "zone" {
		return nil, fmt.Errorf("BuildExcerpts > PassageBoundary allowed values are 'sentence', 'paragraph', and 'zone', now is: %s", opts.PassageBoundary)
	}

	// Default values, all bool values are default false.
	if opts.BeforeMatch == "" {
		opts.BeforeMatch = "<b>"
	}
	if opts.AfterMatch == "" {
		opts.AfterMatch = "</b>"
	}
	if opts.ChunkSeparator == "" {
		opts.ChunkSeparator = "..."
	}
	if opts.HtmlStripMode == "" {
		opts.HtmlStripMode = "index"
	}
	if opts.Limit == 0 {
		opts.Limit = 256
	}
	if opts.Around == 0 {
		opts.Around = 5
	}
	if opts.StartPassageId == 0 {
		opts.StartPassageId = 1
	}

	var req []byte
	req = writeInt32ToBytes(req, 0)

	iFlags := 1 // remove_spaces
	if opts.ExactPhrase != false {
		iFlags |= 2
	}
	if opts.SinglePassage != false {
		iFlags |= 4
	}
	if opts.UseBoundaries != false {
		iFlags |= 8
	}
	if opts.WeightOrder != false {
		iFlags |= 16
	}
	if opts.QueryMode != false {
		iFlags |= 32
	}
	if opts.ForceAllWords != false {
		iFlags |= 64
	}
	if opts.LoadFiles != false {
		iFlags |= 128
	}
	if opts.AllowEmpty != false {
		iFlags |= 256
	}
	if opts.EmitZones != false {
		iFlags |= 256
	}
	req = writeInt32ToBytes(req, iFlags)

	req = writeLenStrToBytes(req, index)
	req = writeLenStrToBytes(req, words)

	req = writeLenStrToBytes(req, opts.BeforeMatch)
	req = writeLenStrToBytes(req, opts.AfterMatch)
	req = writeLenStrToBytes(req, opts.ChunkSeparator)
	req = writeInt32ToBytes(req, opts.Limit)
	req = writeInt32ToBytes(req, opts.Around)
	req = writeInt32ToBytes(req, opts.LimitPassages)
	req = writeInt32ToBytes(req, opts.LimitWords)
	req = writeInt32ToBytes(req, opts.StartPassageId)
	req = writeLenStrToBytes(req, opts.HtmlStripMode)
	req = writeLenStrToBytes(req, opts.PassageBoundary)

	req = writeInt32ToBytes(req, len(docs))
	for _, doc := range docs {
		req = writeLenStrToBytes(req, doc)
	}

	response, err := sc.doRequest(SEARCHD_COMMAND_EXCERPT, VER_COMMAND_EXCERPT, req)
	if err != nil {
		return nil, err
	}

	var bp = byteParser{stream: response}

	resDocs = make([]string, len(docs))
	for i := 0; i < len(docs); i++ {
		resDocs[i] = bp.String()
	}

	return resDocs, nil
}

/*
 Connect to searchd server and update given attributes on given documents in given indexes.
 values[*][0] is docId, must be an uint64.
 values[*][1:] should be int or []int(mva mode)
 'ndocs'	-1 on failure, amount of actually found and updated documents (might be 0) on success
*/
func (sc *Client) UpdateAttributes(index string, attrs []string, values [][]interface{}, ignorenonexistent bool) (ndocs int, err error) {
	if index == "" {
		return -1, errors.New("UpdateAttributes > index name is empty!")
	}
	if len(attrs) == 0 {
		return -1, errors.New("UpdateAttributes > no attribute names provided!")
	}
	if len(values) < 1 {
		return -1, errors.New("UpdateAttributes > no update values provided!")
	}

	for _, v := range values {
		// values[*][0] is docId, so +1
		if len(v) != len(attrs)+1 {
			return -1, fmt.Errorf("UpdateAttributes > update entry has wrong length: %#v", v)
		}
	}

	var mva bool
	if _, ok := values[0][1].([]int); ok {
		mva = true
	}

	// build request
	var req []byte
	req = writeLenStrToBytes(req, index)
	req = writeInt32ToBytes(req, len(attrs))

	if VER_COMMAND_UPDATE > 0x102 {
		if ignorenonexistent {
			req = writeInt32ToBytes(req, 1)
		} else {
			req = writeInt32ToBytes(req, 0)
		}
	}

	for _, attr := range attrs {
		req = writeLenStrToBytes(req, attr)
		if mva {
			req = writeInt32ToBytes(req, 1)
		} else {
			req = writeInt32ToBytes(req, 0)
		}
	}

	req = writeInt32ToBytes(req, len(values))
	for i := 0; i < len(values); i++ {
		if docId, ok := values[i][0].(uint64); !ok {
			return -1, fmt.Errorf("UpdateAttributes > docId must be uint64: %#v", docId)
		} else {
			req = writeInt64ToBytes(req, docId)
		}
		for j := 1; j < len(values[i]); j++ {
			if mva {
				vars, ok := values[i][j].([]int)
				if !ok {
					return -1, fmt.Errorf("UpdateAttributes > must be []int in mva mode: %#v", vars)
				}
				req = writeInt32ToBytes(req, len(vars))
				for _, v := range vars {
					req = writeInt32ToBytes(req, v)
				}
			} else {
				v, ok := values[i][j].(int)
				if !ok {
					return -1, fmt.Errorf("UpdateAttributes > must be int if not in mva mode: %#v", values[i][j])
				}
				req = writeInt32ToBytes(req, v)
			}
		}
	}

	response, err := sc.doRequest(SEARCHD_COMMAND_UPDATE, VER_COMMAND_UPDATE, req)
	if err != nil {
		return -1, err
	}

	ndocs = int(binary.BigEndian.Uint32(response[0:4]))
	return
}

type Keyword struct {
	Tokenized  string "Tokenized"
	Normalized string "Normalized"
	Docs       int
	Hits       int
}

// Connect to searchd server, and generate keyword list for a given query.
// Returns null on failure, an array of Maps with misc per-keyword info on success.
func (sc *Client) BuildKeywords(query, index string, hits bool) (keywords []Keyword, err error) {
	var req []byte
	req = writeLenStrToBytes(req, query)
	req = writeLenStrToBytes(req, index)
	if hits {
		req = writeInt32ToBytes(req, 1)
	} else {
		req = writeInt32ToBytes(req, 0)
	}

	response, err := sc.doRequest(SEARCHD_COMMAND_KEYWORDS, VER_COMMAND_KEYWORDS, req)
	if err != nil {
		return nil, err
	}

	var bp = byteParser{stream: response}

	nwords := bp.Int32()

	keywords = make([]Keyword, nwords)

	for i := 0; i < nwords; i++ {
		var k Keyword
		k.Tokenized = bp.String()

		k.Normalized = bp.String()

		if hits {
			k.Docs = bp.Int32()
			k.Hits = bp.Int32()
		}
		keywords[i] = k
	}

	return
}

func EscapeString(s string) string {
	chars := []string{`\`, `(`, `)`, `|`, `-`, `!`, `@`, `~`, `"`, `&`, `/`, `^`, `$`, `=`}
	for _, char := range chars {
		s = strings.Replace(s, char, `\`+char, -1)
	}
	return s
}

func (sc *Client) Status() (response [][]string, err error) {
	var req []byte
	req = writeInt32ToBytes(req, 1)

	res, err := sc.doRequest(SEARCHD_COMMAND_STATUS, VER_COMMAND_STATUS, req)
	if err != nil {
		return nil, err
	}

	var bp = byteParser{stream: res}

	rows := bp.Uint32()
	cols := bp.Uint32()

	response = make([][]string, rows)
	for i := 0; i < int(rows); i++ {
		response[i] = make([]string, cols)
		for j := 0; j < int(cols); j++ {
			response[i][j] = bp.String()
		}
	}
	return response, nil
}

func (sc *Client) FlushAttributes() (iFlushTag int, err error) {
	res, err := sc.doRequest(SEARCHD_COMMAND_FLUSHATTRS, VER_COMMAND_FLUSHATTRS, []byte{})
	if err != nil {
		return -1, err
	}

	if len(res) != 4 {
		return -1, errors.New("FlushAttributes > unexpected response length!")
	}

	iFlushTag = int(binary.BigEndian.Uint32(res[0:4]))
	return
}

func (sc *Client) connect() (err error) {
	if sc.conn != nil {
		return
	}

	// set connerror to false.
	sc.connerror = false

	timeout := time.Duration(sc.Timeout) * time.Millisecond

	// Try unix socket first.
	if sc.Socket != "" {
		if sc.conn, err = net.DialTimeout("unix", sc.Socket, timeout); err != nil {
			sc.connerror = true
			return fmt.Errorf("connect() net.DialTimeout(%d ms) > %v", sc.Timeout, err)
		}
	} else if sc.Port > 0 {
		if sc.conn, err = net.DialTimeout("tcp", fmt.Sprintf("%s:%d", sc.Host, sc.Port), timeout); err != nil {
			sc.connerror = true
			return fmt.Errorf("connect() net.DialTimeout(%d ms) > %v", sc.Timeout, err)
		}
	} else {
		return fmt.Errorf("connect() > No valid socket or port!\n%Client: #v", sc)
	}

	// Set deadline
	if err = sc.conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		sc.connerror = true
		return fmt.Errorf("connect() conn.SetDeadline() > %v", err)
	}

	header := make([]byte, 4)
	if _, err = io.ReadFull(sc.conn, header); err != nil {
		sc.connerror = true
		return fmt.Errorf("connect() io.ReadFull() > %v", err)
	}

	version := binary.BigEndian.Uint32(header)
	if version < 1 {
		return fmt.Errorf("connect() > expected searchd protocol version 1+, got version %d", version)
	}

	// send my version
	var i int
	i, err = sc.conn.Write(writeInt32ToBytes([]byte{}, VER_MAJOR_PROTO))
	if err != nil {
		sc.connerror = true
		return fmt.Errorf("connect() conn.Write() > %d bytes, %v", i, err)
	}

	sc.conn.SetDeadline(time.Time{})

	return
}

func (sc *Client) Open() (err error) {
	if err = sc.connect(); err != nil {
		return fmt.Errorf("Open > %v", err)
	}

	var req []byte
	req = writeInt16ToBytes(req, SEARCHD_COMMAND_PERSIST)
	req = writeInt16ToBytes(req, 0) // command version
	req = writeInt32ToBytes(req, 4) // body length
	req = writeInt32ToBytes(req, 1) // body

	var n int
	n, err = sc.conn.Write(req)
	if err != nil {
		sc.connerror = true
		return fmt.Errorf("Open > sc.conn.Write() %d bytes, %v", n, err)
	}

	return nil
}

func (sc *Client) Close() error {
	if sc.conn == nil {
		return errors.New("Close > Not connected!")
	}

	if err := sc.conn.Close(); err != nil {
		return err
	}

	sc.conn = nil
	return nil
}

func (sc *Client) doRequest(command int, version int, req []byte) (res []byte, err error) {
	defer func() {
		if x := recover(); x != nil {
			res = nil
			err = fmt.Errorf("doRequest panic > %#v", x)
		}
	}()

	if err = sc.connect(); err != nil {
		return nil, err
	}

	var cmdVerLen []byte
	cmdVerLen = writeInt16ToBytes(cmdVerLen, command)
	cmdVerLen = writeInt16ToBytes(cmdVerLen, version)
	cmdVerLen = writeInt32ToBytes(cmdVerLen, len(req))
	req = append(cmdVerLen, req...)
	_, err = sc.conn.Write(req)
	if err != nil {
		sc.connerror = true
		return nil, fmt.Errorf("conn.Write error: %v", err)
	}

	header := make([]byte, 8)
	if i, err := io.ReadFull(sc.conn, header); err != nil {
		sc.connerror = true
		return nil, fmt.Errorf("doRequest > just read %d bytes into header!", i)
	}

	status := binary.BigEndian.Uint16(header[0:2])
	ver := binary.BigEndian.Uint16(header[2:4])
	size := binary.BigEndian.Uint32(header[4:8])
	if size <= 0 {
		return nil, fmt.Errorf("doRequest > invalid response packet size (len=%d).", size)
	}

	res = make([]byte, size)
	if i, err := io.ReadFull(sc.conn, res); err != nil {
		sc.connerror = true
		return nil, fmt.Errorf("doRequest > just read %d bytes into res (size=%d).", i, size)
	}

	switch status {
	case SEARCHD_OK:
		// do nothing
	case SEARCHD_WARNING:
		wlen := binary.BigEndian.Uint32(res[0:4])
		sc.warning = string(res[4:4+wlen])
		res = res[4+wlen:]
	case SEARCHD_ERROR, SEARCHD_RETRY:
		wlen := binary.BigEndian.Uint32(res[0:4])
		return nil, fmt.Errorf("doRequest > SEARCHD_ERROR: " + string(res[4:4+wlen]))
	default:
		return nil, fmt.Errorf("doRequest > unknown status code (status=%d), ver: %d", status, ver)
	}

	return res, nil
}

func writeFloat32ToBytes(bs []byte, f float32) []byte {
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.BigEndian, f); err != nil {
		fmt.Println(err)
	}
	return append(bs, buf.Bytes()...)
}

func writeInt16ToBytes(bs []byte, i int) []byte {
	var byte2 = make([]byte, 2)
	binary.BigEndian.PutUint16(byte2, uint16(i))
	return append(bs, byte2...)
}

func writeInt32ToBytes(bs []byte, i int) []byte {
	var byte4 = make([]byte, 4)
	binary.BigEndian.PutUint32(byte4, uint32(i))
	return append(bs, byte4...)
}

func writeInt64ToBytes(bs []byte, ui uint64) []byte {
	var byte8 = make([]byte, 8)
	binary.BigEndian.PutUint64(byte8, ui)
	return append(bs, byte8...)
}

func writeLenStrToBytes(bs []byte, s string) []byte {
	var byte4 = make([]byte, 4)
	binary.BigEndian.PutUint32(byte4, uint32(len(s)))
	bs = append(bs, byte4...)
	return append(bs, []byte(s)...)
}

// For SetGeoAnchor()
func DegreeToRadian(degree float32) float32 {
	return degree * math.Pi / 180
}


type byteParser struct {
	stream []byte
	p int
}

func (bp *byteParser) Int32() (i int) {
	i = int(binary.BigEndian.Uint32(bp.stream[bp.p : bp.p+4]))
	bp.p += 4
	return
}

func (bp *byteParser) Uint32() (i uint32) {
	i = binary.BigEndian.Uint32(bp.stream[bp.p : bp.p+4])
	bp.p += 4
	return
}

func (bp *byteParser) Uint64() (i uint64) {
	i = binary.BigEndian.Uint64(bp.stream[bp.p : bp.p+8])
	bp.p += 8
	return
}

func (bp *byteParser) Float32() (f float32, err error) {
	buf := bytes.NewBuffer(bp.stream[bp.p : bp.p + 4])
	bp.p += 4
	if err := binary.Read(buf, binary.BigEndian, &f); err != nil {
		return 0, err
	}
	return f, nil
}

func (bp *byteParser) String() (s string) {
	s = ""
	if slen := bp.Int32(); slen > 0 {
		s = string(bp.stream[bp.p : bp.p+slen])
		bp.p += slen
	}
	return
}
