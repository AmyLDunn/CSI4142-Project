CREATE TABLE dim_demographics (
	demographics_key INTEGER PRIMARY KEY,
	dissemination_area INTEGER,
	population INTEGER,
	median_age FLOAT CHECK (median_age BETWEEN 0 AND 150),
	total_dwellings INTEGER CHECK (total_dwellings>=0),
	average_family_size FLOAT CHECK (average_family_size>=1),
	average_income NUMERIC(11,2) CHECK (average_income >= 0),
	median_income NUMERIC(11,2) CHECK (median_income >= 0),
	mother_tongue_official_percentage FLOAT CHECK (mother_tongue_official_percentage BETWEEN 0 AND 100),
	mother_tongue_unofficial_percentage FLOAT CHECK (mother_tongue_official_percentage BETWEEN 0 AND 100),
	census_year INTEGER CHECK (census_year=2011 OR census_year=2016 OR census_year=2021)
);