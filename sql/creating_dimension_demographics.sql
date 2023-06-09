CREATE TABLE dim_demographics (
	demographics_key BIGINT PRIMARY KEY,
	dissemination_area INTEGER,
	population INTEGER,
	median_age FLOAT CHECK (median_age BETWEEN 0 AND 150),
	total_dwellings FLOAT CHECK (total_dwellings>=0),
	average_household_size FLOAT CHECK (average_household_size>=1),
	median_household_income NUMERIC(11,2) CHECK (median_household_income >= 0),
	mother_tongue_official_percentage FLOAT CHECK (mother_tongue_official_percentage BETWEEN 0 AND 100),
	mother_tongue_unofficial_percentage FLOAT CHECK (mother_tongue_unofficial_percentage BETWEEN 0 AND 100),
	census_year INTEGER CHECK (census_year=2011 OR census_year=2016 OR census_year=2021)
);