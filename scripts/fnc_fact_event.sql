delimiter $$
CREATE  FUNCTION `fnc_fact_event`(p_prd_id integer) RETURNS int
    DETERMINISTIC
BEGIN
    drop temporary table if exists stg0_event;
	create temporary table stg0_event as
    (select
	distinct_id,
	uuid,
	B.id sk_region,
	event,
	str_to_date(replace(replace(event_time,'T',' '),'Z',''),'%Y-%m-%d %H:%i:%s') event_time,
	elements 
    from stg_event A
    left join jala.dim_region B
    on A.city_name = B.city_name
    );
    
    INSERT INTO jala.fact_event(
	distinct_id,
    uuid,
	sk_region,
	event,
	event_time,
	elements,
	load_time
    )
    select
    uuid,
	sk_region,
	event,
	event_time,
	elements,
    current_timestamp()
    from stg0_event;
    
    drop temporary table stg0_event;
    
    
RETURN 1;
END$$

delimiter ;