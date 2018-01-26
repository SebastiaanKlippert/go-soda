package soda

import (
	"encoding/csv"
	"encoding/json"
	"io"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

const (
	apptoken = ""
	endpoint = "https://data.ct.gov/resource/y6p2-px98"
)

func TestGetRequestSerialize(t *testing.T) {

	gr := NewGetRequest(endpoint, apptoken)
	gr.Format = "json"
	gr.Filters["farm_name"] = "Bell Nurseries"
	gr.Filters["item"] = "Salad/micro greens"
	gr.Query.Limit = 1

	want := endpoint + ".json"
	if gr.GetEndpoint() != want {
		t.Errorf("Want %s, have %s", want, gr.GetEndpoint())
	}

	want = "%24limit=1&farm_name=Bell+Nurseries&item=Salad%2Fmicro+greens"
	if gr.URLValues().Encode() != want {
		t.Errorf("Want %s, have %s", want, gr.URLValues().Encode())
	}

	gr.Filters = make(SimpleFilters) //reset filters

	gr.Query.Select = []string{"farm_name", "category", "item", "website"}
	gr.Query.Where = "item like '%ADISH%'"
	gr.Query.Limit = 10
	gr.Query.Offset = 20
	gr.Query.AddOrder("category", DirDesc)
	gr.Query.AddOrder("farm_name", false)

	want = "%24limit=10&%24offset=20&%24order=category+DESC%2Cfarm_name+ASC&%24select=farm_name%2Ccategory%2Citem%2Cwebsite&%24where=item+like+%27%25ADISH%25%27"
	if gr.URLValues().Encode() != want {
		t.Errorf("Want %s, have %s", want, gr.URLValues().Encode())
	}
}

func TestCount(t *testing.T) {
	gr := NewGetRequest(endpoint, apptoken)
	//count all records
	count, err := gr.Count()
	if err != nil {
		t.Fatal(err)
	}
	if count < 22000 {
		t.Fatalf("Expected a count of atleast %d, have %d", 22000, count)
	}
	t.Logf("Count all: %d\n", count)

	//filtered count
	gr.Filters["farm_name"] = "Bell Nurseries"
	gr.Filters["item"] = "Salad/micro greens"
	count, err = gr.Count()
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("Expected a count of %d, have %d", 1, count)
	}
	t.Logf("Count filtered: %d\n", count)
}

func TestFields(t *testing.T) {
	gr := NewGetRequest(endpoint, apptoken)
	fields, err := gr.Fields()
	if err != nil {
		t.Fatal(err)
	}
	if len(fields) < 2 {
		t.Fatalf("Expected atleast %d fields, have %d", 2, len(fields))
	}
	t.Logf("Fields: %v\n", fields)
}

func TestModified(t *testing.T) {
	gr := NewGetRequest(endpoint, apptoken)
	modified, err := gr.Modified()
	if err != nil {
		t.Fatal(err)
	}
	t.Log(modified)
}

func TestGetJSON(t *testing.T) {

	gr := NewGetRequest(endpoint, apptoken)
	gr.Format = "json"
	gr.Query.Where = "item = 'Radishes'"
	gr.Query.Limit = 1000
	gr.Query.AddOrder("category", DirDesc)
	gr.Query.AddOrder("farm_name", DirAsc)

	resp, err := gr.Get()
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	results := []Business{}
	err = json.NewDecoder(resp.Body).Decode(&results)
	if err != nil {
		t.Fatal(err)
	}

	if len(results) == 0 {
		t.Errorf("No results found")
	}
	for _, res := range results {
		if res.Item != "Radishes" {
			t.Errorf("Item %s is not 'Radishes'", res.Item)
		}
	}
	t.Logf("%d JSON results\n", len(results))
}

func TestGetCSV(t *testing.T) {

	gr := NewGetRequest(endpoint, apptoken)
	gr.Format = "csv"
	gr.Query.Select = []string{"category", "farm_name", "item"}
	gr.Filters["farm_name"] = "Beaver Brook Farm"
	gr.Filters["item"] = "Pumpkins"

	resp, err := gr.Get()
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	csvreader := csv.NewReader(resp.Body)
	rows := 0
	for {
		record, err := csvreader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatal(err)
		}
		if rows == 0 {
			rows++
			continue
		}
		want := []string{"Seasonal Items", "Beaver Brook Farm", "Pumpkins"}
		for i := range want {
			if record[i] != want[i] {
				t.Errorf("Want '%v', have '%v'", want[i], record[i])
			}
		}
		rows++
	}
	t.Logf("%d CSV rows\n", rows)
}

func TestMetadataConstructor(t *testing.T) {

	ms := []metadata{
		newMetadata("https://data.ct.gov/resource/y6p2-px98"),
		newMetadata("https://data.ct.gov/resource/y6p2-px98/"),
	}

	for _, m := range ms {
		wb := "https://data.ct.gov"
		wi := "y6p2-px98"
		if m.baseurl != wb {
			t.Errorf("Want baseurl %s, have %s", wb, m.baseurl)
		}
		if m.identifier != wi {
			t.Errorf("Want identifier %s, have %s", wi, m.identifier)
		}
	}

}

func TestGetMetadata(t *testing.T) {

	m := newMetadata(endpoint)

	gr := NewGetRequest(endpoint, apptoken)
	md, err := gr.Metadata.Get()
	if err != nil {
		t.Fatal(err)
	}

	if md.ID != m.identifier {
		t.Errorf("Want ID %s, have %s", m.identifier, md.ID)
	}

	w := "2015-01-23 21:01:23 +0000 UTC"
	if md.CreatedAt.Time().UTC().String() != w {
		t.Errorf("Want CreatedAt %s, have %s", w, md.CreatedAt.Time().UTC().String())
	}

}

func TestGetMetadataError(t *testing.T) {

	gr := NewGetRequest(endpoint[:len(endpoint)-2], apptoken)
	md, err := gr.Metadata.Get()
	if err == nil || md != nil {
		t.Fatal("Wanted error")
	}

}

func TestGetMetadataColumns(t *testing.T) {

	gr := NewGetRequest(endpoint, apptoken)
	cols, err := gr.Metadata.GetColumns()
	if err != nil {
		t.Fatal(err)
	}

	if len(cols) != 16 {
		t.Errorf("Want %d columns, have %d", 16, len(cols))
	}

	want := []string{"Farm Name", "Category", "Item", "Farmer ID", "Location 1 (state)"} //not complete, but good enough
	for _, w := range want {
		found := false
		for _, col := range cols {
			if col.Name == w {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Did not find column %s", w)
		}
	}
}

func TestOffsetGetRequest(t *testing.T) {

	//only run using Travis Go Tip version or when not in Travis
	travisGo := os.Getenv("TRAVIS_GO_VERSION")
	if travisGo != "" && travisGo != "tip" {
		t.Logf("Skipping on go version %s", travisGo)
		return
	}

	gr := NewGetRequest(endpoint, apptoken)
	gr.Format = "json"
	gr.Query.AddOrder("farm_name", DirAsc)

	ogr, err := NewOffsetGetRequest(gr)
	if err != nil {
		t.Fatal(err)
	}

	records := uint64(0)
	start := time.Now()
	numGoroutines := 4
	batchSize := uint(2000)

	for i := 0; i < numGoroutines; i++ {

		ogr.Add(1)
		go func() {
			defer ogr.Done()

			for {
				resp, err := ogr.Next(batchSize)
				if err == ErrDone {
					break
				}
				if err != nil {
					t.Fatal(err)
				}

				results := []Business{}
				err = json.NewDecoder(resp.Body).Decode(&results)
				resp.Body.Close()
				if err != nil {
					t.Fatal(err)
				}
				atomic.AddUint64(&records, uint64(len(results)))
			}
		}()

	}
	ogr.Wait()

	if uint(records) != ogr.Count() {
		t.Errorf("Wanted %d records, have %d", ogr.Count(), records)
	}
	t.Logf("Got %d records in %s using %d goroutines", records, time.Since(start), numGoroutines)
}

type Business struct {
	Business  string `json:"business"`
	Category  string `json:"category"`
	FarmName  string `json:"farm_name"`
	FarmerID  string `json:"farmer_id"`
	Item      string `json:"item"`
	L         string `json:"l"`
	Location1 struct {
		HumanAddress  string `json:"human_address"`
		Latitude      string `json:"latitude"`
		Longitude     string `json:"longitude"`
		NeedsRecoding bool   `json:"needs_recoding"`
	} `json:"location_1"`
	Phone1  string `json:"phone1"`
	Zipcode string `json:"zipcode"`
}
