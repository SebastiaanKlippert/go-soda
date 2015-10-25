package soda

import (
	"encoding/csv"
	"encoding/json"
	"io"
	"os"
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
	gr.Query.AddOrder("category", true)
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
		t.Fatal("Expected a count of atleast %d, have %d", 22000, count)
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
		t.Fatal("Expected a count of %d, have %d", 1, count)
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
		t.Fatal("Expected atleast %d fields, have %d", 2, len(fields))
	}
	t.Logf("Fields: %v\n", fields)
}

func TestGetJSON(t *testing.T) {

	gr := NewGetRequest(endpoint, apptoken)
	gr.Format = "json"
	gr.Query.Where = "item = 'Radishes'"
	gr.Query.Limit = 1000
	gr.Query.AddOrder("category", true)
	gr.Query.AddOrder("farm_name", false)

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
	gr.Query.Select = []string{"farm_name", "category", "item"}
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
		want := []string{"Beaver Brook Farm", "Seasonal Items", "Pumpkins"}
		for i, _ := range want {
			if record[i] != want[i] {
				t.Errorf("Want '%v', have '%v'", want[i], record[i])
			}
		}
		rows++
	}
	t.Logf("%d CSV rows\n", rows)
}

func TestOffsetGetRequest(t *testing.T) {

	//only run using Travis Go Tip version or when not in Travis
	travis_go := os.Getenv("TRAVIS_GO_VERSION")
	if travis_go != "" && travis_go != "tip" {
		t.Logf("Skipping on go version %s", travis_go)
		return
	}

	gr := NewGetRequest(endpoint, apptoken)
	gr.Format = "json"
	gr.Query.AddOrder("zipcode", false)

	ogr, err := NewOffsetGetRequest(gr)
	if err != nil {
		t.Fatal(err)
	}

	records := 0
	start := time.Now()
	num_goroutines := 4
	batch_size := uint(2000)

	for i := 0; i < num_goroutines; i++ {

		ogr.Add(1)
		go func() {
			defer ogr.Done()

			for {
				resp, err := ogr.Next(batch_size)
				if err == ErrDone {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				defer resp.Body.Close()

				results := []Business{}
				err = json.NewDecoder(resp.Body).Decode(&results)
				if err != nil {
					t.Fatal(err)
				}
				records += len(results)
			}
		}()

	}
	ogr.Wait()

	if uint(records) != ogr.Count() {
		t.Errorf("Wanted %d records, have %d", ogr.Count(), records)
	}
	t.Logf("Got %d records in %s using %d goroutines", records, time.Since(start), num_goroutines)
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
