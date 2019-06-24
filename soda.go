// Package soda provides HTTP GET tools for SODA (Socrata Open Data API) webservices, see http://dev.socrata.com/
package soda

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

type HTTPRequester interface {
	Do(req *http.Request) (*http.Response, error)
}

// GetRequest is a wrapper/container for SODA requests.
// This is NOT safe for use by multiple goroutines as Format, Filters and Query will be overwritten.
// Create a new GetRequest in each goroutine you use or use an OffsetGetRequest
type GetRequest struct {
	apptoken   string
	endpoint   string //endpoint without format (not .json etc at the end)
	Format     string //json, csv etc
	Filters    SimpleFilters
	Query      SoSQL
	Metadata   metadata
	HTTPClient HTTPRequester //For clients who need a custom HTTP client
}

// NewGetRequest creates a new GET request, the endpoint must be specified without the format.
// For example https://data.ct.gov/resource/hma6-9xbg
func NewGetRequest(endpoint, apptoken string) *GetRequest {
	return &GetRequest{
		apptoken: apptoken,
		endpoint: endpoint,
		Filters:  make(SimpleFilters),
		Metadata: newMetadata(endpoint),
	}
}

// Get executes the HTTP GET request
func (r *GetRequest) Get() (*http.Response, error) {
	//If offset is used we must specify an order
	if r.Query.Offset > 0 && len(r.Query.Order) == 0 {
		return nil, errors.New("cannot use an offset without setting the order")
	}
	return get(r, r.URLValues().Encode())
}

// GetEndpoint returns the complete SODA URL with format
func (r *GetRequest) GetEndpoint() string {
	if r.Format == "" {
		r.Format = "json"
	}
	return fmt.Sprintf("%s.%s", r.endpoint, r.Format)
}

// URLValues returns the url.Values for the GetRequest
func (r *GetRequest) URLValues() url.Values {
	uv := make(url.Values)
	for key, val := range r.Filters.URLValues() {
		uv[key] = val
	}
	for key, val := range r.Query.URLValues() {
		uv[key] = val
	}
	return uv
}

// Count gets the total number of records in the dataset
// by executing a SODA request
func (r *GetRequest) Count() (uint, error) {

	oldformat := r.Format
	oldorder := r.Query.Order
	oldselect := r.Query.Select
	defer func() {
		r.Format = oldformat
		r.Query.Order = oldorder
		r.Query.Select = oldselect
	}()

	r.Format = "json"
	r.Query.Select = []string{"count(*)"}
	r.Query.ClearOrder()

	resp, err := r.Get()
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	count := make([]struct {
		Count string
	}, 0)
	err = json.NewDecoder(resp.Body).Decode(&count)
	if err != nil {
		return 0, err
	}
	if len(count) == 0 {
		return 0, errors.New("empty count response")
	}
	icount, err := strconv.Atoi(count[0].Count)
	if err != nil {
		return 0, err
	}
	return uint(icount), nil
}

// Fields returns all the fields present in the dataset (ignores select fields).
// Spaces in fieldnames are replaced by underscores.
func (r *GetRequest) Fields() ([]string, error) {

	oldformat := r.Format
	oldorder := r.Query.Order
	oldlimit := r.Query.Limit
	oldselect := r.Query.Select
	defer func() {
		r.Format = oldformat
		r.Query.Select = oldselect
		r.Query.Order = oldorder
		r.Query.Limit = oldlimit
	}()

	r.Format = "csv"
	r.Query.Select = []string{}
	r.Query.Limit = 0
	r.Query.ClearOrder()

	resp, err := r.Get()
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	csvreader := csv.NewReader(resp.Body)
	record, err := csvreader.Read()
	if err != nil && err != io.EOF {
		return nil, err
	}
	fields := make([]string, len(record))
	for i := range record {
		fields[i] = strings.Replace(record[i], " ", "_", -1)
	}
	return fields, nil
}

// Modified returns when the dataset was last updated
func (r *GetRequest) Modified() (time.Time, error) {

	oldformat := r.Format
	oldorder := r.Query.Order
	oldlimit := r.Query.Limit
	oldselect := r.Query.Select
	defer func() {
		r.Format = oldformat
		r.Query.Select = oldselect
		r.Query.Order = oldorder
		r.Query.Limit = oldlimit
	}()

	r.Format = "json"
	r.Query.Select = []string{}
	r.Query.Limit = 0
	r.Query.ClearOrder()

	resp, err := r.Get()
	if err != nil {
		return time.Time{}, err
	}
	defer resp.Body.Close()

	lms := resp.Header.Get("X-Soda2-Truth-Last-Modified")
	if lms == "" {
		lms = resp.Header.Get("Last-Modified")
	}
	if lms == "" {
		return time.Time{}, errors.New("cannot get last modified date, field not present in HTTP header")
	}

	return time.Parse(time.RFC1123, lms)
}

// SimpleFilters is the easiest way to filter columns for equality.
// Add the column to filter on a map key and the filter value as map value.
// If you include multiple filters, the filters will be combined using a boolean AND.
// See http://dev.socrata.com/docs/filtering.html
type SimpleFilters map[string]string

//URLValues returns the url.Values for the SimpleFilters
func (sf SimpleFilters) URLValues() url.Values {
	uv := make(url.Values)
	for key, val := range sf {
		uv.Add(key, val)
	}
	return uv
}

// SoSQL implements the Socrata Query Language and is used to build more complex queries.
// See http://dev.socrata.com/docs/queries.html
type SoSQL struct {
	Select []string //The set of columns to be returned. Default: All columns, equivalent to $select=*
	Where  string   //Filters the rows to be returned. Default: No filter, and returning a max of $limit values
	Order  []struct {
		Column string //Column name
		Desc   bool   //Descending. Default: false = Ascending
	} //Specifies the order of results. Default: Unspecified order, but it will be consistent across paging
	Group  string //Column to group results on, similar to SQL Grouping. Default: No grouping
	Limit  uint   //Maximum number of results to return. Default: 1000 (with a maximum of 50,000)
	Offset uint   //Offset count into the results to start at, used for paging. Default: 0
	Q      string //Performs a full text search for a value. Default: No search

}

// Direction is used to set the sort direction to ascending or descending
type Direction bool

const (
	// DirAsc is used to set ascending sort order
	DirAsc Direction = false

	// DirDesc is used to set descending sort order
	DirDesc Direction = true
)

// AddOrder can be called for each field you want to sort the result on.
// If parameter descending is true, the column will be sorted descending, or ascending if false.
func (sq *SoSQL) AddOrder(column string, dir Direction) {
	sq.Order = append(sq.Order, struct {
		Column string
		Desc   bool
	}{column, bool(dir)})
}

// ClearOrder removes all order fields
func (sq *SoSQL) ClearOrder() {
	sq.Order = []struct {
		Column string
		Desc   bool
	}{}
}

// URLValues returns the url.Values for the SoSQL query
func (sq *SoSQL) URLValues() url.Values {
	uv := make(url.Values)
	if len(sq.Select) > 0 {
		uv.Add("$select", strings.Join(sq.Select, ","))
	}
	if len(sq.Where) > 0 {
		uv.Add("$where", sq.Where)
	}
	if len(sq.Order) > 0 {
		order := make([]string, 0)
		for _, o := range sq.Order {
			if o.Desc {
				order = append(order, o.Column+" DESC")
			} else {
				order = append(order, o.Column+" ASC")
			}
		}
		uv.Add("$order", strings.Join(order, ","))
	}
	if len(sq.Q) > 0 {
		uv.Add("$q", sq.Q)
	}
	if len(sq.Group) > 0 {
		uv.Add("$group", sq.Group)
	}
	if sq.Limit > 0 {
		uv.Add("$limit", fmt.Sprintf("%d", sq.Limit))
	}
	if sq.Offset > 0 {
		uv.Add("$offset", fmt.Sprintf("%d", sq.Offset))
	}
	return uv
}

// OffsetGetRequest is a request getter that gets all the records using the filters and limits from gr and
// is safe to use by multiple goroutines, use Next(number) to get the next number of records.
// A sync.WaitGroup is embedded for easy concurrency.
type OffsetGetRequest struct {
	gr     *GetRequest
	m      sync.Mutex
	offset uint
	count  uint
	sync.WaitGroup
}

// ErrDone is returned by OffsetGetRequest.Next when done
var ErrDone = errors.New("Done")

// Next gets the next number of records
func (o *OffsetGetRequest) Next(number uint) (*http.Response, error) {
	o.m.Lock() //lock to protect offset
	if o.IsDone() {
		o.m.Unlock()
		return nil, ErrDone
	}
	if len(o.gr.Query.Order) == 0 { //If offset is used we must specify an order
		return nil, errors.New("cannot use an offset without setting the order")
	}
	if o.offset+number > o.count {
		number = o.count - o.offset
	}
	o.gr.Query.Offset = o.offset
	o.gr.Query.Limit = number
	rawquery := o.gr.URLValues().Encode()
	o.offset += number
	o.m.Unlock() //unlock before the request is done
	return get(o.gr, rawquery)
}

// Count returns the number of records from memory
func (o *OffsetGetRequest) Count() uint {
	return o.count
}

// IsDone returns if we have gotten all records
func (o *OffsetGetRequest) IsDone() bool {
	return o.offset >= o.count
}

// NewOffsetGetRequest creates a new OffsetGetRequest from gr
// and does a count request to determine the number of records to get
func NewOffsetGetRequest(gr *GetRequest) (*OffsetGetRequest, error) {
	count, err := gr.Count()
	if err != nil {
		return nil, err
	}
	return &OffsetGetRequest{gr: gr, offset: 0, count: count}, nil
}

// get is the function that executes the HTTP request
func get(r *GetRequest, rawquery string) (*http.Response, error) {

	var client HTTPRequester = http.DefaultClient
	if r.HTTPClient != nil {
		client = r.HTTPClient
	}

	req, err := http.NewRequest("GET", r.GetEndpoint(), nil)
	if err != nil {
		return nil, err
	}
	req.URL.RawQuery = rawquery
	req.Header.Set("X-App-Token", r.apptoken)

	// Execute
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode >= 400 {
		errMsg, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("SODA error %d:\nURL: GET %s\nResponse: %s", resp.StatusCode, req.URL.String(), errMsg)
	}

	return resp, nil
}
