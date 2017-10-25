package transitdb

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/golang/protobuf/ptypes"
	pb "github.com/maxhawkins/transitdb/proto"
)

type ListQuotesRequest struct {
	StartDate    time.Time `json:"startDate"`
	EndDate      time.Time `json:"endDate"`
	Origins      []string  `json:"origins"`
	Destinations []string  `json:"destinations"`
	Limit        int       `json:"limit"`
	Offset       int       `json:"offset"`
}

func (l *ListQuotesRequest) FromHTTP(r *http.Request) error {
	startDate, err := time.Parse("2006-01-02", r.FormValue("start"))
	if err != nil {
		return errors.New("invalid 'start'")
	}
	endDate, err := time.Parse("2006-01-02", r.FormValue("end"))
	if err != nil {
		return errors.New("invalid 'end'")
	}
	origins := r.Form["origin"]
	dests := r.Form["dest"]

	limit, _ := strconv.Atoi(r.FormValue("limit"))
	if limit == 0 {
		limit = 100
	}

	offset, _ := strconv.Atoi(r.FormValue("offset"))

	l.StartDate = startDate
	l.EndDate = endDate
	l.Origins = origins
	l.Destinations = dests
	l.Limit = limit
	l.Offset = offset

	return nil
}

type Quote struct {
	Cost          int    `json:"cost"`
	Origin        string `json:"origin"`
	OriginCountry string `json:"originCountry"`
	Dest          string `json:"dest"`
	DestCountry   string `json:"destCountry"`
	Date          Date   `json:"date"`
}

type Date time.Time

func (d *Date) UnmarshalJSON(data []byte) error {
	t, err := time.Parse(`"2006-01-02"`, string(data))
	if err != nil {
		return err
	}
	*d = Date(t)
	return nil
}

func (d Date) MarshalJSON() ([]byte, error) {
	formatted := time.Time(d).Format(`"2006-01-02"`)
	return []byte(formatted), nil
}

type Offer struct {
	ID int `json:"id,omitempty"`

	// OriginID      int `json:"originID,omitempty"`
	// DestinationID int `json:"destinationID,omitempty"`

	OriginAirport      string `json:"originAirport,omitempty"`
	DestinationAirport string `json:"destinationAirport,omitempty"`

	Cost   int    `json:"cost"`
	Source string `json:"source"`

	AvailableFrom Date `json:"availableFrom,omitempty"`
	AvailableTo   Date `json:"availableTo,omitempty"`

	OfferedAt time.Time `json:"offeredAt"`
	ExpiresAt time.Time `json:"expiresAt,omitempty"`
}

func (o *Offer) ToProto() (*pb.Offer, error) {
	startTimePb, err := ptypes.TimestampProto(time.Time(o.AvailableFrom))
	if err != nil {
		return nil, err
	}

	endTimePb, err := ptypes.TimestampProto(time.Time(o.AvailableTo))
	if err != nil {
		return nil, err
	}

	createdAtPb, err := ptypes.TimestampProto(o.OfferedAt)
	if err != nil {
		return nil, err
	}

	return &pb.Offer{
		Origin:      o.OriginAirport,
		Destination: o.DestinationAirport,
		Cost:        int32(o.Cost),
		StartTime:   startTimePb,
		EndTime:     endTimePb,
		CreatedAt:   createdAtPb,
	}, nil
}

func (o *Offer) Validate() error {
	// if o.OriginID <= 0 {
	// 	return errors.New("missing originID")
	// }
	// if o.DestinationID <= 0 {
	// 	return errors.New("missing destinationID")
	// }
	//
	// TODO(maxhawkins): validate airport junk
	if o.Cost <= 0 {
		return errors.New("missing cost")
	}
	if o.Source == "" {
		return errors.New("missing source")
	}
	if o.OfferedAt.IsZero() {
		return errors.New("missing offeredAt")
	}
	if time.Time(o.AvailableFrom).IsZero() {
		return errors.New("missing availableFrom")
	}
	return nil
}
