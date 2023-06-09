CREATE TABLE dim_location (
	location_key INTEGER PRIMARY KEY,
	longitude NUMERIC(9,6) CHECK (longitude BETWEEN -180 AND 180), -- ###.######
	latitude NUMERIC(9,6) CHECK (latitude BETWEEN -90 AND 90), -- ###.######
	nearest_intersection TEXT,
	floor_level SMALLINT CHECK (floor_level > 0),
	postal_code VARCHAR(6), -- LNLNLN
	dissemination_area SMALLINT
);