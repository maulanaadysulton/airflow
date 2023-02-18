CREATE DATABASES jala;
USE jala;
DROP TABLE IF EXISTS dim_region;
CREATE TABLE dim_region(
id bigint primary key not null AUTO_INCREMENT,
city_name varchar(100),
sub_division_code varchar(5),
sub_division_name varchar(100),
country_code varchar(5),
country_name varchar(100),
continent_code varchar(5),
continent_name varchar(100),
load_time timestamp
);

drop table if exists fact_event;
CREATE TABLE fact_event(
id bigint primary key not null auto_increment,
uuid varchar(100),
sk_region bigint,
event varchar(100),
event_time timestamp,
elements json,
load_time timestamp,
INDEX idx_region (sk_region),
INDEX idx_event (event),
FOREIGN KEY (sk_region)
      REFERENCES dim_region(id)
);

drop table if exists stg_event;
create table stg_event(
distinct_id varchar(100),
event varchar(100),
event_time varchar(50),
uuid varchar(100),
elements json,
city_name varchar(100),
country_name varchar(100),
country_code varchar(5),
continent_name varchar(100),
continent_code varchar(5),
latitude float,
longitude float,
time_zone varchar(100),
subdivision_code varchar(5),
subdivision_name varchar(100)
);
