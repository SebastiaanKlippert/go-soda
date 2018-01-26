[![GoDoc](https://godoc.org/github.com/golang/gddo?status.svg)](http://godoc.org/github.com/SebastiaanKlippert/go-soda)
[![Build Status](https://travis-ci.org/SebastiaanKlippert/go-soda.svg?branch=master)](https://travis-ci.org/SebastiaanKlippert/go-soda)
[![codecov](https://codecov.io/gh/SebastiaanKlippert/go-soda/branch/master/graph/badge.svg)](https://codecov.io/gh/SebastiaanKlippert/go-soda)
[![Go Report Card](https://goreportcard.com/badge/SebastiaanKlippert/go-soda)](https://goreportcard.com/report/SebastiaanKlippert/go-soda)


# go-soda

Socrata Open Data API (SODA) GET client for Golang

![SODA Logo](http://1j3rac4ejwve1p3y0x1gprgk.wpengine.netdna-cdn.com/wp-content/uploads/2010/11/soda1.png)

For SODA docs see http://dev.socrata.com

## Features

This is a simple client for get requests only.
The client provides basic structs for querying and filtering the database.
Although all operations are supported most are just provided as a string to allow for all kind of complex queries.

## Install

Just go get it
```
go get -u github.com/SebastiaanKlippert/go-soda
```

## GetRequest

The default GetRequest struct is not safe for use in multiple goroutines, create one for each goroutine or use the OffsetGetRequest.

## OffsetGetRequest

The OffsetGetRequest is a wrapper around the GetRequest and provides an easy offset counter to get loads of data. 
It can be shared by multiple goroutines to get your data a lot faster.

## Metadata

For each GetRequest you can request metadata (using a separate API call). The metadata contains info about 
the dataset like creation and update times, licensing info and advanced column info.

```go
sodareq := soda.NewGetRequest("https://data.ct.gov/resource/y6p2-px98", "")
metadata, err := sodareq.Metadata.Get()
```

## GetRequest sample

See the test file for more examples.

```go
func QuerySample() {

	sodareq := soda.NewGetRequest("https://data.ct.gov/resource/y6p2-px98", "")

	//count all records
	count, err := sodareq.Count()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(count)
	
	//get dataset last updated time
	modified, err := sodareq.Modified()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(modified)	

	//list all fields/columns
	fields, err := sodareq.Fields()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(fields)

	//get some JSON data using a complex query
	sodareq.Format = "json"
	sodareq.Query.Select = []string{"farm_name", "category", "item", "zipcode"}
	sodareq.Query.Where = "lower(farm_name) like '%sun%farm%' AND (item in('Radishes', 
	  'Cucumbers') OR lower(item) like '%flower%')"
	sodareq.Query.Limit = 1000
	sodareq.Query.AddOrder("farm_name", soda.DirAsc)
	sodareq.Query.AddOrder("category", soda.DirDesc)

	//count this result first
	querycount, err := sodareq.Count()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(querycount)

	//get the results
	resp, err := sodareq.Get()
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	rawresp, _ := ioutil.ReadAll(resp.Body)
	fmt.Println(string(rawresp))
}

func JSONSample() {

	sodareq := soda.NewGetRequest("https://data.ct.gov/resource/y6p2-px98", "")

	//get some JSON data
	sodareq.Format = "json"
	sodareq.Filters["item"] = "Radishes"
	sodareq.Query.Limit = 10

	resp, err := sodareq.Get()
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	results := make([]map[string]interface{}, 0)
	err = json.NewDecoder(resp.Body).Decode(&results)
	if err != nil {
		log.Fatal(err)
	}

	//Process data here
	for _, r := range results {
		fmt.Println(r["farm_name"], r["item"])
	}
}

func CSVSample() {
	sodareq := soda.NewGetRequest("https://data.ct.gov/resource/y6p2-px98", "")
	sodareq.Format = "csv"
	sodareq.Filters["item"] = "Radishes"
	sodareq.Query.Limit = 10

	resp, err := sodareq.Get()
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	//Process data here
	csvreader := csv.NewReader(resp.Body)
	for {
		record, err := csvreader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(record)
	}
}
```

## OffsetGetRequest sample

Get all data in batches of 2000 rows using 4 goroutines

```go
func GetAllData() error {

	gr := soda.NewGetRequest("https://data.ct.gov/resource/y6p2-px98", "")
	gr.Format = "json"
	gr.Query.AddOrder("zipcode", soda.DirAsc)

	ogr, err := soda.NewOffsetGetRequest(gr)
	if err != nil {
		return err
	}

	for i := 0; i < 4; i++ {

		ogr.Add(1)
		go func() {
			defer ogr.Done()

			for {
				resp, err := ogr.Next(2000)
				if err == soda.ErrDone {
					break
				}
				if err != nil {
					log.Fatal(err)
				}

				results := make([]map[string]interface{}, 0)
				err = json.NewDecoder(resp.Body).Decode(&results)
				resp.Body.Close()
				if err != nil {
					log.Fatal(err)
				}
				//Process your data
			}
		}()

	}
	ogr.Wait()

	return nil
}
```
