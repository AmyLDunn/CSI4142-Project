CREATE TABLE dim_fire_ward (
	fire_ward_key INTEGER PRIMARY KEY,
	stations_in_ward INTEGER CHECK (stations_in_ward>0)
);
