package soda

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type metadata struct {
	baseurl, identifier string
}

func (m metadata) url() (string, error) {
	if m.baseurl == "" || len(m.identifier) != 9 || m.identifier[4] != '-' {
		return "", fmt.Errorf("Cannot get metadata, is the resource URL used correct?")
	}
	return fmt.Sprintf("%s/views/%s", m.baseurl, m.identifier), nil
}

func (m metadata) do() (*Metadata, error) {
	url, err := m.url()
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		b, _ := ioutil.ReadAll(resp.Body)
		return nil, fmt.Errorf("Received statuscode %d\nBody: %s", resp.StatusCode, b)
	}

	md := new(Metadata)
	err = json.NewDecoder(resp.Body).Decode(md)
	if err != nil {
		return nil, err
	}
	return md, nil
}

//Get gets the metadata struct for this dataset
func (m metadata) Get() (*Metadata, error) {
	return m.do()
}

//GetColumns gets only the column info from the metadata for this dataset
func (m metadata) GetColumns() ([]Column, error) {
	md, err := m.do()
	if err != nil {
		return []Column{}, err
	}
	return md.Columns, nil
}

//newMetadata splits a resource url like https://data.ct.gov/resource/hma6-9xbg
//into https://data.ct.gov and hma6-9xbg
func newMetadata(resourceurl string) metadata {

	m := metadata{}

	u, err := url.Parse(resourceurl)
	if err != nil {
		return m
	}

	m.baseurl = fmt.Sprintf("%s://%s", u.Scheme, u.Host)

	ps := strings.Split(strings.TrimSuffix(u.Path, "/"), "/")
	m.identifier = ps[len(ps)-1]

	return m
}

//Format describes column formats
type Format struct {
	PrecisionStyle string `json:"precisionStyle"`
	Align          string `json:"align"`
	NoCommas       string `json:"noCommas"`
}

//Column describes one data column
type Column struct {
	DataTypeName   string `json:"dataTypeName"`
	FieldName      string `json:"fieldName"`
	Format         Format `json:"format"`
	ID             int    `json:"id"`
	Name           string `json:"name"`
	Position       int    `json:"position"`
	RenderTypeName string `json:"renderTypeName"`
	TableColumnID  int    `json:"tableColumnId"`
	Width          int    `json:"width"`
}

//Metadata contains the resource metadata
type Metadata struct {
	AverageRating int       `json:"averageRating"`
	Category      string    `json:"category"`
	Columns       []Column  `json:"columns"`
	CreatedAt     Timestamp `json:"createdAt"`
	DisplayType   string    `json:"displayType"`
	DownloadCount int       `json:"downloadCount"`
	Flags         []string  `json:"flags"`
	Grants        []struct {
		Flags     []string `json:"flags"`
		Inherited bool     `json:"inherited"`
		Type      string   `json:"type"`
	} `json:"grants"`
	ID             string    `json:"id"`
	IndexUpdatedAt Timestamp `json:"indexUpdatedAt"`
	License        struct {
		Name string `json:"name"`
	} `json:"license"`
	LicenseID string `json:"licenseId"`
	Metadata  struct {
		AvailableDisplayTypes []string `json:"availableDisplayTypes"`
		CustomFields          struct {
			Licentie struct {
				Licentie string `json:"Licentie"`
			} `json:"Licentie"`
		} `json:"custom_fields"`
		RdfSubject       string `json:"rdfSubject"`
		RenderTypeConfig struct {
			Visible struct {
				Table bool `json:"table"`
			} `json:"visible"`
		} `json:"renderTypeConfig"`
		RowLabel string `json:"rowLabel"`
	} `json:"metadata"`
	Name             string `json:"name"`
	NewBackend       bool   `json:"newBackend"`
	NumberOfComments int    `json:"numberOfComments"`
	Oid              int    `json:"oid"`
	Owner            struct {
		DisplayName string   `json:"displayName"`
		ID          string   `json:"id"`
		Rights      []string `json:"rights"`
		RoleName    string   `json:"roleName"`
		ScreenName  string   `json:"screenName"`
	} `json:"owner"`
	PublicationAppendEnabled bool      `json:"publicationAppendEnabled"`
	PublicationDate          Timestamp `json:"publicationDate"`
	PublicationGroup         int       `json:"publicationGroup"`
	PublicationStage         string    `json:"publicationStage"`
	Query                    struct{}  `json:"-"` //TODO
	Ratings                  struct {
		Rating int `json:"rating"`
	} `json:"ratings"`
	Rights        []string  `json:"rights"`
	RowsUpdatedAt Timestamp `json:"rowsUpdatedAt"`
	RowsUpdatedBy string    `json:"rowsUpdatedBy"`
	TableAuthor   struct {
		DisplayName string   `json:"displayName"`
		ID          string   `json:"id"`
		Rights      []string `json:"rights"`
		RoleName    string   `json:"roleName"`
		ScreenName  string   `json:"screenName"`
	} `json:"tableAuthor"`
	TableID          int       `json:"tableId"`
	Tags             []string  `json:"tags"`
	TotalTimesRated  int       `json:"totalTimesRated"`
	ViewCount        int       `json:"viewCount"`
	ViewLastModified Timestamp `json:"viewLastModified"`
	ViewType         string    `json:"viewType"`
}

//Timestamp is a time.Time struct unmarshalled from a unix epoch time
type Timestamp time.Time

//UnmarshalJSON sets t from a timestamp
func (t *Timestamp) UnmarshalJSON(b []byte) error {
	n, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return err
	}
	*t = Timestamp(time.Unix(n, 0))
	return nil
}

//Time returns t as time.Time
func (t Timestamp) Time() time.Time {
	return time.Time(t)
}
