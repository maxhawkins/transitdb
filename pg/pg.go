package pg

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/lib/pq"
	"github.com/maxhawkins/transitdb"
)

func Open(url string) (*Store, error) {
	db, err := sql.Open("postgres", url)
	if err != nil {
		return nil, err
	}

	_, err = db.Exec(schema)
	if err != nil {
		return nil, err
	}

	return &Store{
		db: db,
	}, nil
}

const schema = `
CREATE TABLE IF NOT EXISTS
places (
    place_id   SERIAL        PRIMARY KEY,
    latitude   DECIMAL       NOT NULL,
    longitude  DECIMAL       NOT NULL,
    name       VARCHAR(100)  NOT NULL,
    country    VARCHAR(2)    NOT NULL,
    iata_code  VARCHAR(3)
);

CREATE UNIQUE INDEX IF NOT EXISTS
place_airport_idx ON places (iata_code);

CREATE TABLE IF NOT EXISTS
offers (
    offer_id    SERIAL       PRIMARY KEY,
    origin_id   INT          NOT NULL
                             REFERENCES places(place_id),
    dest_id     INT          NOT NULL
                             REFERENCES places(place_id),
    cost        DECIMAL      NOT NULL,
    source      VARCHAR(20)  NOT NULL,
    start_time  DATE         NOT NULL,
    end_time    DATE,
    created_at  TIMESTAMP    NOT NULL,
    expires_at  TIMESTAMP
);

CREATE INDEX IF NOT EXISTS
offer_cost_join_idx
ON offers (origin_id, dest_id, start_time, expires_at, cost);

CREATE INDEX IF NOT EXISTS
offer_date_idx
ON OFFERS (start_time);
`

type Store struct {
	db *sql.DB
}

func (s *Store) Close() error {
	return s.db.Close()
}

var (
	airportIDCache   = make(map[string]int)
	airportIDCacheMu sync.Mutex
)

func (s *Store) AirportIDByIATA(ctx context.Context, iata string) (int, error) {
	id, ok := airportIDCache[iata]
	if ok {
		return id, nil
	}

	row := s.db.QueryRowContext(ctx,
		`SELECT place_id FROM places WHERE iata_code = $1`,
		iata)
	if err := row.Scan(&id); err != nil {
		return id, err
	}

	airportIDCacheMu.Lock()
	airportIDCache[iata] = id
	airportIDCacheMu.Unlock()

	return id, nil
}

func (s *Store) SaveOffer(ctx context.Context, o transitdb.Offer) error {
	availableFrom := pq.NullTime{
		Time:  time.Time(o.AvailableFrom),
		Valid: !time.Time(o.AvailableFrom).IsZero(),
	}
	availableTo := pq.NullTime{
		Time:  time.Time(o.AvailableTo),
		Valid: !time.Time(o.AvailableTo).IsZero(),
	}

	expiresAt := pq.NullTime{Time: o.ExpiresAt, Valid: !o.ExpiresAt.IsZero()}

	_, err := s.db.ExecContext(ctx, `
			INSERT INTO offers
			(origin_id, dest_id, cost, source, start_time, end_time, created_at, expires_at)
			VALUES
			(
				(SELECT place_id FROM places WHERE iata_code = $1),
				(SELECT place_id FROM places WHERE iata_code = $2),
				$3, $4, $5, $6, $7, $8
			)`,
		o.OriginAirport,
		o.DestinationAirport,
		o.Cost,
		o.Source,
		availableFrom,
		availableTo,
		o.OfferedAt,
		expiresAt)

	if err, ok := err.(*pq.Error); ok {
		isNullErr := err.Code.Name() == "not_null_violation"
		if isNullErr && err.Column == "origin_id" {
			return fmt.Errorf("unknown origin airport %q", o.OriginAirport)
		}
		if isNullErr && err.Column == "dest_id" {
			return fmt.Errorf("unknown destination airport %q", o.DestinationAirport)
		}
	}

	if err != nil {
		return err
	}

	return nil
}

func (s *Store) CheapestPerRoute(ctx context.Context, start, end time.Time) ([]transitdb.Quote, error) {
	rows, err := s.db.QueryContext(ctx, cheapestPerRouteSQL, start, end)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []transitdb.Quote
	for rows.Next() {
		var res transitdb.Quote

		err = rows.Scan(
			&res.Origin,
			&res.OriginCountry,
			&res.Dest,
			&res.DestCountry,
			&res.Cost,
			&res.Date)
		if err != nil {
			return nil, err
		}

		results = append(results, res)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

const cheapestPerRouteSQL = `
WITH

cheapest AS (
    SELECT origin_id,
		   dest_id,
		   MIN(start_time) as start_time,
           MIN(cost) AS cost
    FROM offers
    WHERE start_time BETWEEN $1 AND $2
    GROUP BY (origin_id, dest_id)
)

SELECT origin.iata_code,
       origin.country,
	   dest.iata_code,
	   dest.country,
	   cheapest.cost,
	   cheapest.start_time
FROM cheapest
JOIN places AS origin
	ON origin.place_id = cheapest.origin_id
JOIN places AS dest
	ON dest.place_id = cheapest.dest_id
ORDER BY cheapest.cost ASC
`

func (s *Store) ListQuotes(ctx context.Context, q transitdb.ListQuotesRequest) ([]transitdb.Quote, error) {
	rows, err := s.db.QueryContext(ctx, listQuotesSQL,
		q.StartDate, q.EndDate,
		strings.Join(q.Origins, ","),
		strings.Join(q.Destinations, ","),
		q.Limit,
		q.Offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []transitdb.Quote
	for rows.Next() {
		var res transitdb.Quote

		err = rows.Scan(
			&res.Cost,
			&res.Origin,
			&res.OriginCountry,
			&res.Dest,
			&res.DestCountry,
			&res.Date)
		if err != nil {
			return nil, err
		}

		results = append(results, res)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

const listQuotesSQL = `
WITH

-- Get offers in our time range from the source airport, ignoring
-- ones that don't match our conditions.
-- 
matching_offers AS (
    SELECT offers.*
      FROM offers
           JOIN places AS origin
              ON origin.place_id = offers.origin_id
           JOIN places AS dest
              ON dest.place_id = offers.dest_id
     WHERE (start_time BETWEEN $1 AND $2)
       AND ($3 = '' OR origin.iata_code = ANY(string_to_array($3, ',')))
       AND ($4 = '' OR dest.iata_code = ANY(string_to_array($4, ',')))
       AND expires_at > NOW()
),

-- Set of offers for a given leg that have the lowest price during
-- the time window. There may be more than one.
--
min_offers AS (
      SELECT origin_id,
             dest_id,
             MIN(cost) AS cost
        FROM matching_offers
    GROUP BY (origin_id, dest_id)
),

-- The offer that's happening the soonest with a minimal cost
--
next_min_offer AS (
      SELECT origin_id,
             dest_id,
             cost,
             MIN(start_time) AS start_time
        FROM min_offers
             JOIN matching_offers USING (origin_id, dest_id, cost)
    GROUP BY (origin_id, dest_id, cost)
)

-- Print them all, starting with the cheapest
--
SELECT
	cost,
	origin.name,
	origin.country,
	dest.name,
	dest.country,
	start_time AS cheapest_date
FROM next_min_offer
     JOIN offers USING (origin_id, dest_id, cost, start_time)
     JOIN places AS dest
          ON dest.place_id = offers.dest_id
     JOIN places AS origin
          ON origin.place_id = offers.origin_id
ORDER BY cost ASC
LIMIT $5
OFFSET $6;
`
