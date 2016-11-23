WITH

-- Get offers in our time range from the
-- source airport, ignoring countries i've
-- already been to
matching_offers AS (
    SELECT offers.*
      FROM offers
           JOIN places AS origin
              ON origin.place_id = offers.origin_id
           JOIN places AS dest
              ON dest.place_id = offers.dest_id
     WHERE start_time BETWEEN '2016-12-01' AND '2017-02-01'
       AND origin.iata_code IN ('DSM')
       -- AND origin.country IN ('US')
       -- AND origin.iata_code IN ('LAS', 'SFO', 'OAK', 'SJC')
       -- AND expires_at > NOW()
       AND offers.cost < 500
),

-- Set of offers for a given leg that have
-- the lowest price during the time window.
-- There may be more than one.
min_offers AS (
      SELECT origin_id,
             dest_id,
             MIN(cost) AS cost
        FROM matching_offers
    GROUP BY (origin_id, dest_id)
),

-- The offer that's happening the soonest
-- with a minimal cost
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
  SELECT cost, offers.name, dest.name, dest.country, start_time AS cheapest_date
    FROM next_min_offer
         JOIN offers USING (origin_id, dest_id, cost, start_time)
         JOIN places AS dest
              ON dest.place_id = offers.dest_id
         JOIN places AS origin
              ON origin.place_id = offers.origin_id
ORDER BY cost ASC;
