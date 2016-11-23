package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/jmoiron/sqlx"
)

type Date struct {
	time.Time
}

func (d *Date) UnmarshalJSON(data []byte) error {
	t, err := time.Parse(`"2006-01-02"`, string(data))
	if err != nil {
		return err
	}
	*d = Date{t}
	return nil
}

func (d *Date) MarshalJSON() ([]byte, error) {
	formatted := d.Format(`"2006-01-02"`)
	return []byte(formatted), nil
}

type Offer struct {
	ID int `json:"id,omitempty"`

	// OriginID      int `json:"originID,omitempty"`
	// DestinationID int `json:"destinationID,omitempty"`

	OriginAirport      string `json:"originAirport,omitempty"`
	DestinationAirport string `json:"destinationAirport,omitempty"`

	Cost        int    `json:"cost"`
	Description string `json:"description"`
	Source      string `json:"source"`

	AvailableFrom Date `json:"availableFrom,omitempty"`
	AvailableTo   Date `json:"availableTo,omitempty"`

	OfferedAt time.Time `json:"offeredAt"`
	ExpiresAt time.Time `json:"expiresAt,omitempty"`
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
	if o.AvailableFrom.IsZero() {
		return errors.New("missing availableFrom")
	}
	return nil
}

type API struct {
	db *sqlx.DB
}

func (a *API) HandleAddOffers(w http.ResponseWriter, r *http.Request) {
	var saved int

	scanner := bufio.NewScanner(r.Body)
	for line := 1; scanner.Scan(); line++ {
		var offer Offer
		if err := json.Unmarshal(scanner.Bytes(), &offer); err != nil {
			msg := fmt.Sprintf("line %d: bad json", line)
			http.Error(w, msg, http.StatusBadRequest)
			return
		}

		if err := offer.Validate(); err != nil {
			msg := fmt.Sprintf("line %d: %v", line, err)
			http.Error(w, msg, http.StatusBadRequest)
			return
		}

		if err := (SaveOfferCommand{offer}.Exec(a.db)); err != nil {
			fmt.Fprintln(os.Stderr, "[error]", err)
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}

		saved++
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "[error]", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, "saved %d records\n", saved)
}

func (a *API) HandleGetCheapestRoute(w http.ResponseWriter, r *http.Request) {
	startDate := time.Now()
	endDate := time.Now().Add(24 * time.Hour * 30)

	query := GetCheapestRouteQuery{
		StartDate: startDate,
		EndDate:   endDate,
	}

	resp, err := query.Exec(a.db)
	if err != nil {
		fmt.Fprintln(os.Stderr, "[error]", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	for _, r := range resp {
		fmt.Fprintf(w, "%s,%s,%d\n", r.Origin, r.Dest, r.Cost)
	}
}

func (a *API) HandleGetCheapest(w http.ResponseWriter, r *http.Request) {
	startDate, err := time.Parse("2006-01-02", r.FormValue("start"))
	if err != nil {
		http.Error(w, "invalid 'start'", http.StatusBadRequest)
		return
	}
	endDate, err := time.Parse("2006-01-02", r.FormValue("end"))
	if err != nil {
		http.Error(w, "invalid 'end'", http.StatusBadRequest)
		return
	}
	origins := r.Form["origin"]
	dests := r.Form["dest"]

	limit, _ := strconv.Atoi(r.FormValue("limit"))
	offset, _ := strconv.Atoi(r.FormValue("offset"))

	query := GetCheapestQuery{
		StartDate:    startDate,
		EndDate:      endDate,
		Origins:      origins,
		Destinations: dests,
		Limit:        limit,
		Offset:       offset,
	}

	res, err := query.Exec(a.db)
	if err != nil {
		fmt.Fprintln(os.Stderr, "[error]", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	data, err := json.MarshalIndent(res, "", "\t")
	if err != nil {
		fmt.Fprintln(os.Stderr, "[error]", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func main() {
	var (
		dbPath = flag.String("db", "postgres://localhost/transitdb?sslmode=disable", "db location")
		port   = flag.Int("port", 5030, "http port")
	)
	flag.Parse()

	rand.Seed(time.Now().UnixNano())

	db := sqlx.MustConnect("postgres", *dbPath)
	defer db.Close()

	if err := SetupTables(db); err != nil {
		log.Fatal(err)
	}

	api := &API{db}

	m := mux.NewRouter()
	m.HandleFunc("/offers", api.HandleAddOffers).Methods("POST")
	m.HandleFunc("/cheapest", api.HandleGetCheapestRoute).Methods("GET")

	var handler http.Handler
	handler = handlers.LoggingHandler(os.Stderr, m)

	addr := fmt.Sprint(":", *port)
	fmt.Fprintln(os.Stderr, "listening at", addr)
	log.Fatal(http.ListenAndServe(addr, handler))
}
