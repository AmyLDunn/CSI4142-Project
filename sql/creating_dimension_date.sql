CREATE TYPE part_of_day AS ENUM ('night', -- 12am to before 6am
								 'morning', -- 6am to before 12pm
                                 'afternoon', -- 12pm to before 6pm
								 'evening' -- 6pm to before 12am
								 );
CREATE TABLE dim_date (
	date_key INTEGER PRIMARY KEY,
	datetime TIMESTAMP CHECK (datetime BETWEEN '2011-01-01 00:00:00' AND '2019-12-31 23:59:59'),
	year SMALLINT CHECK (year BETWEEN 2011 AND 2019),
	month SMALLINT CHECK (month BETWEEN 1 AND 12),
	day SMALLINT CHECK (day BETWEEN 1 AND 31),
	day_of_week SMALLINT CHECK (day_of_week BETWEEN 1 AND 7),
	weekend BOOLEAN,
	hour SMALLINT CHECK (hour BETWEEN 0 AND 23),
	minute SMALLINT CHECK (minute BETWEEN 0 AND 59),
	holiday VARCHAR(20),
	holiday BOOLEAN,
	time_of_day part_of_day
);