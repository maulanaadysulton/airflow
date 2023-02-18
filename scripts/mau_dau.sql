# MAU
SELECT EXTRACT(YEAR_MONTH FROM event_time),
count(distinct distinct_id) 'MAU'
from fact_event
where EXTRACT(YEAR_MONTH FROM event_time) >= EXTRACT(YEAR_MONTH FROM current_date() - interval 1 month)
and EXTRACT(YEAR_MONTH FROM event_time) < EXTRACT(YEAR_MONTH FROM current_date())
and REGEXP_LIKE(event, 'login','i')
group by 1;


# DAU
SELECT EXTRACT(DAY FROM event_time),
count(distinct distinct_id) 'DAU'
from fact_event
where EXTRACT(DAY FROM event_time) >= EXTRACT(DAY FROM current_date() - interval 1 month)
and EXTRACT(DAY FROM event_time) < EXTRACT(DAY FROM current_date())
and REGEXP_LIKE(event, 'login','i')
group by 1;

# user stickiness
with mau as
(SELECT EXTRACT(YEAR_MONTH FROM event_time) month_year,
count(distinct distinct_id) 'MAU'
from fact_event
where EXTRACT(YEAR_MONTH FROM event_time) >= EXTRACT(YEAR_MONTH FROM current_date() - interval 1 month)
and EXTRACT(YEAR_MONTH FROM event_time) < EXTRACT(YEAR_MONTH FROM current_date())
and REGEXP_LIKE(event, 'login','i')
group by 1)

SELECT 
EXTRACT(YEAR_MONTH FROM event_time) month_year,
EXTRACT(DAY FROM event_time) day,
count(distinct distinct_id)/B.mau 'stickiness'
from fact_event A
left join mau B on A.month_year = B.month_year
where EXTRACT(DAY FROM event_time) >= EXTRACT(DAY FROM current_date() - interval 1 month)
and EXTRACT(DAY FROM event_time) < EXTRACT(DAY FROM current_date())
and REGEXP_LIKE(event, 'login','i')
group by 1