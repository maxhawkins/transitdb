package main

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

type LookupAirportQuery struct {
	IATACode string
}

var (
	airportIDCache   = make(map[string]int)
	airportIDCacheMu sync.Mutex
)

func (q LookupAirportQuery) Exec(queryer sqlx.Queryer) (int, error) {
	id, ok := airportIDCache[q.IATACode]
	if ok {
		return id, nil
	}

	row := queryer.QueryRowx(
		`SELECT place_id FROM places WHERE iata_code = $1`,
		q.IATACode)
	if err := row.Scan(&id); err != nil {
		return id, err
	}

	airportIDCacheMu.Lock()
	airportIDCache[q.IATACode] = id
	airportIDCacheMu.Unlock()

	return id, nil
}

type SaveOfferCommand struct {
	Offer Offer
}

func (s SaveOfferCommand) Exec(e sqlx.Execer) error {
	o := s.Offer

	availableFrom := pq.NullTime{Time: o.AvailableFrom.Time, Valid: !o.AvailableFrom.IsZero()}
	availableTo := pq.NullTime{Time: o.AvailableTo.Time, Valid: !o.AvailableTo.IsZero()}

	expiresAt := pq.NullTime{Time: o.ExpiresAt, Valid: !o.ExpiresAt.IsZero()}

	_, err := e.Exec(`
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

const getCheapestRouteSQL = `
WITH

cheapest AS (
    SELECT origin_id,
           dest_id,
           MIN(cost) AS cost
    FROM offers
    WHERE start_time BETWEEN $1 AND $2
    GROUP BY (origin_id, dest_id)
)

SELECT origin.iata_code,
       dest.iata_code,
       cheapest.cost
FROM cheapest
JOIN places AS origin
	ON origin.place_id = cheapest.origin_id
JOIN places AS dest
	ON dest.place_id = cheapest.dest_id
ORDER BY cheapest.cost ASC
`

type GetCheapestRouteQuery struct {
	StartDate time.Time
	EndDate   time.Time
}

type GetCheapestRouteResult struct {
	Cost   int
	Origin string
	Dest   string
}

func (q GetCheapestRouteQuery) Exec(queryer sqlx.Queryer) ([]GetCheapestRouteResult, error) {
	rows, err := queryer.Query(getCheapestRouteSQL,
		q.StartDate, q.EndDate)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []GetCheapestRouteResult
	for rows.Next() {
		var res GetCheapestRouteResult

		err = rows.Scan(
			&res.Origin,
			&res.Dest,
			&res.Cost)
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

const getCheapestSQL = `
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
SELECT cost, dest.name, dest.country, start_time AS cheapest_date
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

type GetCheapestQuery struct {
	StartDate    time.Time
	EndDate      time.Time
	Origins      []string
	Destinations []string
	Limit        int
	Offset       int
}

type GetCheapestResult struct {
	Cost         int
	Dest         string
	DestCountry  string
	CheapestDate time.Time
}

func (q GetCheapestQuery) Exec(queryer sqlx.Queryer) ([]GetCheapestResult, error) {
	rows, err := queryer.Query(getCheapestSQL,
		q.StartDate, q.EndDate,
		strings.Join(q.Origins, ","),
		strings.Join(q.Destinations, ","),
		q.Limit,
		q.Offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []GetCheapestResult
	for rows.Next() {
		var res GetCheapestResult

		err = rows.Scan(
			&res.Cost,
			&res.Dest, &res.DestCountry,
			&res.CheapestDate)
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
    created_at  TIMESTAMP     NOT NULL,
    expires_at  TIMESTAMP
);

CREATE INDEX IF NOT EXISTS
offer_cost_join_idx
ON offers (origin_id, dest_id, start_time, expires_at, cost);

CREATE INDEX IF NOT EXISTS
offer_date_idx
ON OFFERS (start_time);

`

func SetupTables(e sqlx.Execer) error {
	_, err := e.Exec(schema)
	if err != nil {
		return err
	}

	return nil
}
