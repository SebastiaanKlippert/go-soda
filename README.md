[![GoDoc](https://godoc.org/github.com/golang/gddo?status.svg)](http://godoc.org/github.com/SebastiaanKlippert/go-soda)
[![Build Status](https://travis-ci.org/SebastiaanKlippert/go-soda.svg?branch=master)](https://travis-ci.org/SebastiaanKlippert/go-soda)


# go-soda

Socrata Open Data API (SODA) GET client for Golang

![SODA Logo](http://1j3rac4ejwve1p3y0x1gprgk.wpengine.netdna-cdn.com/wp-content/uploads/2010/11/soda1.png)

For SODA docs see http://dev.socrata.com

## Features

This is a simple client for get requests only.
The client provides basic structs for querying and filtering the database.
Although all operations are supported most are just provided as a string to allow for all kind of complex queries.

## GetRequest

The default GetRequest struct is not safe for use in multiple goroutines, create one for each goroutine or use the OffsetGetRequest.

## OffsetGetRequest

The OffsetGetRequest is a wrapper around the GetRequest and provides an easy offset counter to get loads of data. 
It can be shared by multiple goroutines to get your data a lot faster.

## GetRequest sample

See the test file for more examples.

```go
func JSONSample() {

	sodareq := soda.NewGetRequest("https://data.ct.gov/resource/hma6-9xbg", "")

	//count all records
	count, err := sodareq.Count()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(count)

	//list all fields/columns
	fields, err := sodareq.Fields()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(fields)

	//get some JSON data
	sodareq.Format = "json"
	sodareq.Query.Where = "item = 'Radishes'"
	sodareq.Query.Limit = 100
	sodareq.Query.AddOrder("category", true)
	sodareq.Query.AddOrder("farm_name", false)

	resp, err := sodareq.Get()
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	results := make(map[string]interface{})
	err = json.NewDecoder(resp.Body).Decode(&results)
	if err != nil {
		log.Fatal(err)
	}

	//Process data here
}

func CSVSample() {

	sodareq := soda.NewGetRequest("https://data.ct.gov/resource/hma6-9xbg", "")
	sodareq.Format = "csv"
	sodareq.Filters["item"] = "Radishes"
	sodareq.Query.Limit = 100

	resp, err := sodareq.Get()
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	csvreader := csv.NewReader(resp.Body)

	//Process data here
}
```

## OffsetGetRequest sample

Get all data in batches of 2000 rows using 4 goroutines

```go

func GetAllData() error {

	gr := soda.NewGetRequest("https://data.ct.gov/resource/hma6-9xbg", "")
	gr.Format = "json"
	gr.Query.AddOrder("zipcode", false)

	ogr, err := soda.NewOffsetGetRequest(gr)
	if err != nil {
		return err
	}

	wg := new(sync.WaitGroup)
	for i := 0; i < 4; i++ {

		wg.Add(1)
		go func() {
			defer wg.Done()

			for {
				resp, err := ogr.Next(2000)
				if err == soda.ErrDone {
					break
				}
				if err != nil {
					log.Fatal(err)
				}
				defer resp.Body.Close()

				results := make(map[string]interface{})
				err = json.NewDecoder(resp.Body).Decode(&results)
				if err != nil {
					log.Fatal(err)
				}
				//Process your data
			}
		}()

	}
	wg.Wait()
	return nil
}
```
