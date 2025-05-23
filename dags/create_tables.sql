DROP TABLE IF EXISTS `sigma-heuristic-457716-n7.base.raw_green_trips`;

CREATE TABLE IF NOT EXISTS `sigma-heuristic-457716-n7.base.raw_green_trips`
(
    VendorID INT64 OPTIONS (description = 'A code indicating the LPEP provider that provided the record. 1= Creative Mobile Technologies, LLC; 2= VeriFone Inc.'),
    lpep_pickup_datetime TIMESTAMP OPTIONS (description = 'The date and time when the meter was engaged'),
    lpep_dropoff_datetime TIMESTAMP OPTIONS (description = 'The date and time when the meter was disengaged'),
    store_and_fwd_flag STRING OPTIONS (description = 'This flag indicates whether the trip record was held in vehicle memory before sending to the vendor, aka "store and forward," because the vehicle did not have a connection to the server. Y= store and forward trip N= not a store and forward trip'),
    RatecodeID INT64 OPTIONS (description = 'The final rate code in effect at the end of the trip. 1= Standard rate 2= JFK 3= Newark 4= Nassau or Westchester 5= Negotiated fare 6= Group ride'),
    PULocationID INT64 OPTIONS (description = 'TLC Taxi Zone in which the taximeter was engaged'),
    DOLocationID INT64 OPTIONS (description = 'TLC Taxi Zone in which the taximeter was disengaged'),
    passenger_count INT64 OPTIONS (description = 'The number of passengers in the vehicle. This is a driver-entered value.'),
    trip_distance FLOAT64 OPTIONS (description = 'The elapsed trip distance in miles reported by the taximeter.'),
    fare_amount NUMERIC OPTIONS (description = 'The time-and-distance fare calculated by the meter'),
    extra NUMERIC OPTIONS (description = 'Miscellaneous extras and surcharges. Currently, this only includes the $0.50 and $1 rush hour and overnight charges'),
    mta_tax NUMERIC OPTIONS (description = '$0.50 MTA tax that is automatically triggered based on the metered rate in use'),
    tip_amount NUMERIC OPTIONS (description = 'Tip amount. This field is automatically populated for credit card tips. Cash tips are not included.'),
    tolls_amount NUMERIC OPTIONS (description = 'Total amount of all tolls paid in trip.'),
    ehail_fee NUMERIC OPTIONS (description = 'Fee for e-hail services, if applicable'),
    improvement_surcharge NUMERIC OPTIONS (description = '$0.30 improvement surcharge assessed on hailed trips at the flag drop. The improvement surcharge began being levied in 2015.'),
    total_amount NUMERIC OPTIONS (description = 'The total amount charged to passengers. Does not include cash tips.'),
    payment_type INT64 OPTIONS (description = 'A numeric code signifying how the passenger paid for the trip. 1= Credit card 2= Cash 3= No charge 4= Dispute 5= Unknown 6= Voided trip'),
    trip_type INT64 OPTIONS (description = 'A code indicating whether the trip was a street-hail or a dispatch that is automatically assigned based on the metered rate in use but can be altered by the driver. 1= Street-hail 2= Dispatch'),
    congestion_surcharge NUMERIC OPTIONS (description = 'Congestion surcharge applied to trips in congested zones'),
    yearmonth INT64 OPTIONS (description = 'Year and month of the trip based on lpep_dropoff_datetime, formatted as YYYYMM')
)
PARTITION BY DATE(lpep_pickup_datetime); 


DROP TABLE IF EXISTS `sigma-heuristic-457716-n7.base.raw_yellow_trips`;

CREATE TABLE IF NOT EXISTS `sigma-heuristic-457716-n7.base.raw_yellow_trips`
(
    VendorID INT64 OPTIONS (description = 'A code indicating the LPEP provider that provided the record. 1= Creative Mobile Technologies, LLC; 2= VeriFone Inc.'),
    tpep_pickup_datetime TIMESTAMP OPTIONS (description = 'The date and time when the meter was engaged'),
    tpep_dropoff_datetime TIMESTAMP OPTIONS (description = 'The date and time when the meter was disengaged'),
    passenger_count INT64 OPTIONS (description = 'The number of passengers in the vehicle. This is a driver-entered value.'),
    trip_distance FLOAT64 OPTIONS (description = 'The elapsed trip distance in miles reported by the taximeter.'),
    RatecodeID INT64 OPTIONS (description = 'The final rate code in effect at the end of the trip. 1= Standard rate 2= JFK 3= Newark 4= Nassau or Westchester 5= Negotiated fare 6= Group ride'),
    store_and_fwd_flag STRING OPTIONS (description = 'This flag indicates whether the trip record was held in vehicle memory before sending to the vendor, aka "store and forward," because the vehicle did not have a connection to the server. Y= store and forward trip N= not a store and forward trip'),
    PULocationID INT64 OPTIONS (description = 'TLC Taxi Zone in which the taximeter was engaged'),
    DOLocationID INT64 OPTIONS (description = 'TLC Taxi Zone in which the taximeter was disengaged'),
    payment_type INT64 OPTIONS (description = 'A numeric code signifying how the passenger paid for the trip. 1= Credit card 2= Cash 3= No charge 4= Dispute 5= Unknown 6= Voided trip'),
    fare_amount NUMERIC OPTIONS (description = 'The time-and-distance fare calculated by the meter'),
    extra NUMERIC OPTIONS (description = 'Miscellaneous extras and surcharges. Currently, this only includes the $0.50 and $1 rush hour and overnight charges'),
    mta_tax NUMERIC OPTIONS (description = '$0.50 MTA tax that is automatically triggered based on the metered rate in use'),
    tip_amount NUMERIC OPTIONS (description = 'Tip amount. This field is automatically populated for credit card tips. Cash tips are not included.'),
    tolls_amount NUMERIC OPTIONS (description = 'Total amount of all tolls paid in trip.'),
    improvement_surcharge NUMERIC OPTIONS (description = '$0.30 improvement surcharge assessed on hailed trips at the flag drop. The improvement surcharge began being levied in 2015.'),
    total_amount NUMERIC OPTIONS (description = 'The total amount charged to passengers. Does not include cash tips.'),
    congestion_surcharge NUMERIC OPTIONS (description = 'Congestion surcharge applied to trips in congested zones'),
    yearmonth INT64 OPTIONS (description = 'Year and month of the trip based on lpep_dropoff_datetime, formatted as YYYYMM')
)
PARTITION BY DATE(tpep_pickup_datetime);

