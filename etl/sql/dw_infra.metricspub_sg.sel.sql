select cast (logdate as char(10)) as log_date
, cast ('D' as char(1)) as freq_ind
, cast (205 as smallint) as prd_id
, trim (cast ('# Queries' as char(60))) as met_nm
, cast (count(*) as bigint) as query_count
, cast ('' as char(1)) as dim_nm
, cast ('' as char(1)) as dim_val
from dw_monitor_views.dbqlogtbl_hst
where logdate between '2013-01-01' and date-1
group by 1

union all

select cast ((logdate - td_day_of_week(logdate) + 1) as char(10)) as log_date
, cast ('W' as char(1)) as freq_ind
, cast (205 as smallint) as prod_id
, trim (cast ('# Unique Users' as char(60))) as met_nm
, cast (count(distinct username) as bigint) as unique_user_cnt
, cast ('' as char(1)) as dim_nm
, cast ('' as char(1)) as dim_val
from dw_monitor_views.dbqlogtbl_hst
where logdate between '2013-01-01' and (date - td_day_of_week(date) + 1) - 1
group by 1

order by 2,1
