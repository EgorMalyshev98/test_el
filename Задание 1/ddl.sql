DROP TABLE IF EXISTS stg_user_stats;
CREATE TABLE IF NOT EXISTS stg_user_stats
(
	id SERIAL PRIMARY KEY,
	username varchar,
	likes int,
	views int DEFAULT 0, -- для тестового берем только лайки
	shares int DEFAULT 0, -- для тестового берем только лайки
	media_source varchar,
	load_timestamp timestamp,
	hdiff varchar
);

DROP TABLE IF EXISTS dim_user CASCADE;
CREATE TABLE dim_user (
    user_id SERIAL PRIMARY KEY,
    username TEXT NOT NULL UNIQUE
);

DROP TABLE IF EXISTS dim_source CASCADE;
CREATE TABLE dim_source (
    source_id SERIAL PRIMARY KEY,
    source_name TEXT NOT NULL UNIQUE
);

DROP TABLE IF EXISTS fact_activity;
CREATE TABLE fact_activity (
    fact_id SERIAL PRIMARY KEY,
    user_id INT NOT NULL REFERENCES dim_user(user_id),
    source_id INT NOT NULL REFERENCES dim_source(source_id),
    load_timestamp timestamp,
    likes INT,
    shares INT,
    views INT,
    hdiff varchar
);




