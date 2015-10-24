//package soda provides HTTP GET tools for SODA (Socrata Open Data API) webservices, see http://dev.socrata.com/
package soda

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
)

var ErrDone = fmt.Errorf("Done")

//A wrapper/container for SODA requests
//This is NOT safe for use by multiple goroutines as Format, Filters and Query will be overwritten
//Create a new GetRequest in each goroutine you use or use an OffsetGetRequest
type GetRequest struct {
	apptoken string
	endpoint string //endpoint without format (not .json etc at the end)
	Format   string //json, csv etc
	Filters  SimpleFilters
	Query    SoSQL
}

//Create a new GET request, the endpoint must be specified without the format
//For example https://data.ct.gov/resource/hma6-9xbg
func NewGetRequest(endpoint, apptoken string) *GetRequest {
	return &GetRequest{
		apptoken: apptoken,
		endpoint: endpoint,
		Filters:  make(SimpleFilters),
	}
}

//Execute Get request
func (r *GetRequest) Get() (*http.Response, error) {
	return get(r, r.URLValues().Encode())
}

func (r *GetRequest) GetEndpoint() string {
	if r.Format == "" {
		r.Format = "json"
	}
	return fmt.Sprintf("%s.%s", r.endpoint, r.Format)
}

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

//Counts the total number of records in the dataset
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

	count := []struct {
		Count string
	}{}
	err = json.NewDecoder(resp.Body).Decode(&count)
	if err != nil {
		return 0, err
	}
	if len(count) == 0 {
		return 0, fmt.Errorf("Empty count response")
	}
	icount, err := strconv.Atoi(count[0].Count)
	if err != nil {
		return 0, err
	}
	return uint(icount), err
}

//Gets all the fields present in the dataset, ignores
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

	fields := []string{}

	r.Format = "csv"
	r.Query.Select = []string{}
	r.Query.Limit = 0
	r.Query.ClearOrder()

	resp, err := r.Get()
	if err != nil {
		return fields, err
	}
	defer resp.Body.Close()

	csvreader := csv.NewReader(resp.Body)
	for {
		record, err := csvreader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return fields, err
		}
		fields = record
		break
	}
	return fields, nil
}

//http://dev.socrata.com/docs/filtering.html
type SimpleFilters map[string]string

func (sf SimpleFilters) URLValues() url.Values {
	uv := make(url.Values)
	for key, val := range sf {
		uv.Add(key, val)
	}
	return uv
}

//http://dev.socrata.com/docs/queries.html
type SoSQL struct {
	Select []string //The set of columns to be returned. Default: All columns, equivalent to $select=*
	Where  string   //Filters the rows to be returned. Default: No filter, and returning a max of $limit values
	Order  []struct {
		Column string
		Desc   bool
	} //Specifies the order of results. Default: Unspecified order, but it will be consistent across paging
	Group  string //Column to group results on, similar to SQL Grouping. Default: 	No grouping
	Limit  uint   //Maximum number of results to return. Default: 1000 (with a maximum of 50,000)
	Offset uint   //Offset count into the results to start at, used for paging. Default: 	0
	Q      string //Performs a full text search for a value. Default: 	No search

}

func (sq *SoSQL) AddOrder(column string, descending bool) {
	sq.Order = append(sq.Order, struct {
		Column string
		Desc   bool
	}{column, descending})
}

func (sq *SoSQL) ClearOrder() {
	sq.Order = []struct {
		Column string
		Desc   bool
	}{}
}

func (sq *SoSQL) URLValues() url.Values {
	uv := make(url.Values)
	if len(sq.Select) > 0 {
		uv.Add("$select", strings.Join(sq.Select, ","))
	}
	if len(sq.Where) > 0 {
		uv.Add("$where", sq.Where)
	}
	if len(sq.Order) > 0 {
		order := []string{}
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

//An OffsetGetRequest is a request getter that gets all the records using the filters and limits from gr and
//is safe to use by multiple goroutines, use Next(number) to get the next number of records
type OffsetGetRequest struct {
	gr     *GetRequest
	m      sync.Mutex
	offset uint
	count  uint
}

//Gets the next number of records
func (o *OffsetGetRequest) Next(number uint) (*http.Response, error) {
	if o.offset >= o.count {
		return nil, ErrDone
	}
	o.m.Lock() //lock to protect offset
	o.gr.Query.Offset = o.offset
	o.gr.Query.Limit = number
	rawquery := o.gr.URLValues().Encode()
	o.offset += number
	o.m.Unlock() //unlock before the request is done
	return get(o.gr, rawquery)
}

func (o *OffsetGetRequest) Count() uint {
	return o.count
}

//returns if we have gotten all records
func (o *OffsetGetRequest) Done() bool {
	return o.offset >= o.count
}

//Creates a new OffsetGetRequest from gr and does a count request to determine the number of records to get
func NewOffsetGetRequest(gr *GetRequest) (*OffsetGetRequest, error) {
	count, err := gr.Count()
	if err != nil {
		return nil, err
	}
	return &OffsetGetRequest{gr: gr, offset: 0, count: count}, nil
}

func get(r *GetRequest, rawquery string) (*http.Response, error) {

	//If offset is used we must specify an order
	if r.Query.Offset > 0 && len(r.Query.Order) == 0 {
		return nil, fmt.Errorf("Cannot use an offset without setting the order")
	}

	req, err := http.NewRequest("GET", r.GetEndpoint(), nil)
	if err != nil {
		return nil, err
	}
	req.URL.RawQuery = rawquery
	req.Header.Set("X-App-Token", r.apptoken)

	//Execute
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode >= 400 {
		errormessage, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("SODA error %d:\n:%s", resp.StatusCode, errormessage)
	}

	return resp, nil
}
