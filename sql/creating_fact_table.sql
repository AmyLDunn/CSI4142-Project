CREATE TYPE causes AS ENUM ('intentional',
                            'unintentional');

CREATE TYPE sprinkler AS ENUM ('no sprinkler system',
                               'sprinkler system present and operated',
							   'sprinkler system present, did not operate',
							   'sprinkler system present, operation undetermined',
							   'sprinkler system presence undetermined');

CREATE TYPE smoke_alarm AS ENUM ('no smoke alarm',
                                 'smoke alarm present and operated',
						         'smoke alarm present, did not operate',
						         'smoke alarm present, operation undetermined',
						         'smoke alarm presence undetermined');

CREATE TYPE fire_alarm AS ENUM ('no fire alarm',
                                'fire alarm present and operated',
						        'fire alarm present, did not operate',
						        'fire alarm present, operation undetermined',
						        'fire alarm presence undetermined');


CREATE TABLE fact_fire_incidents (
	location_key BIGINT FOREIGN KEY dim_location(location_key),
	date_key BIGINT FOREIGN KEY dim_date(date_key),
	weather_key BIGINT FOREIGN KEY dim_weather(weather_key),
	demographics_key BIGINT FOREIGN KEY dim_demographics(demographics_key),
	fire_ward_key INTEGER FOREIGN KEY dim_fire_ward(fire_ward_key),
	response_time SMALLINT CHECK (response_time >= 0),
	damage_cad NUMERIC(11,2) CHECK (damage_cad >= 0),
	casualties SMALLINT CHECK (casualties >= 0),
	people_displaced SMALLINT CHECK (people_displaced >= 0),
	people_rescued SMALLINT CHECK (people_rescued >= 0),
	responding_personel SMALLINT CHECK (responding_personel >= 0),
	responding_apparatus SMALLINT CHECK (responding_apparatus >= 0),
	possible_cause causes,
	sprinkler_stystem sprinkler,
	smoke_system smoke_alarm,
	fire_system fire_alarm
);