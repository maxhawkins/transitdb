package transitdb

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/gorilla/mux"
)

type Handler struct {
	Store  Store
	Router *mux.Router
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.Router == nil {
		r := mux.NewRouter()
		r.HandleFunc("/offers", h.HandleAddOffers).Methods("POST")
		r.HandleFunc("/quotes", h.HandleListQuotes).Methods("GET")
		r.HandleFunc("/quotes/cheapest", h.HandleCheapestPerRoute).Methods("GET")
		h.Router = r
	}

	h.Router.ServeHTTP(w, r)
}

func (h *Handler) HandleAddOffers(w http.ResponseWriter, r *http.Request) {
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

		if err := h.Store.SaveOffer(r.Context(), offer); err != nil {
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

func (h *Handler) HandleCheapestPerRoute(w http.ResponseWriter, r *http.Request) {
	startDate := time.Now()
	endDate := time.Now().Add(24 * time.Hour * 30)

	resp, err := h.Store.CheapestPerRoute(r.Context(), startDate, endDate)
	if err != nil {
		fmt.Fprintln(os.Stderr, "[error]", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	for _, r := range resp {
		fmt.Fprintf(w, "%s,%s,%d\n", r.Origin, r.Dest, r.Cost)
	}
}

func (h *Handler) HandleListQuotes(w http.ResponseWriter, r *http.Request) {
	var query ListQuotesRequest
	if err := query.FromHTTP(r); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	fmt.Println(query)

	res, err := h.Store.ListQuotes(r.Context(), query)
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
