delimiter $$
CREATE FUNCTION `fnc_dim_region`(p_prd_id integer) RETURNS int
    DETERMINISTIC
BEGIN
	drop temporary table if exists stg_region;
    create temporary table stg_region as
	select A.*,
	case when B.id is null then 1 else 0 end as new_ind
	from
		(select
		city_name,
		subdivision_code,
		subdivision_name,
		country_code,
		country_name,
		continent_code,
		continent_name
		from stg_event) A
		left join jala.dim_region B on A.city_name = B.city_name;

	insert into jala.dim_region (
	city_name,
	sub_division_code,
	sub_division_name,
	country_code,
	country_name,
	continent_code,
	continent_name,
	load_time
	)
	select
	city_name,
	subdivision_code,
	subdivision_name,
	country_code,
	country_name,
	continent_code,
	continent_name,
	current_timestamp()
	from stg_region where new_ind = 1;
    
    drop temporary table stg_region;
RETURN 1;
END$$

delimiter ;