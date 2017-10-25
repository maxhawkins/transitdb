package transitdb

import (
	"context"
	"time"
)

type Store interface {
	AirportIDByIATA(ctx context.Context, iata string) (int, error)
	SaveOffer(context.Context, Offer) error
	CheapestPerRoute(ctx context.Context, start, end time.Time) ([]Quote, error)
	ListQuotes(context.Context, ListQuotesRequest) ([]Quote, error)
}
