select cast (start_date as char(10)) as start_date
     , cast ('D' as char(1)) as freq_ind
     , cast (203 as smallint) as prd_id
     , trim (cast ('# custom scripts executed per day' as char(60))) as met_nm
     , count(*) as job_count
     , cast ('' as char(1)) as dim_nm
     , cast ('' as char(1)) as dim_val
from da_views.dw_infra_etl_log
where called_script = '/dw/etl/home/prod/bin/shell_handler.ksh'
and subject_area <> 'dw_infra'
and start_date between current_date - 60 and current_date - 1
group by 1,2,3,4,6,7

union all

select cast (start_date as char(10)) as start_date
     , cast ('D' as char(1)) as freq_ind
     , cast (203 as smallint) as prd_id
     , trim (cast ('# UC4 jobs with UoW parms per day' as char(60))) as met_nm
     , count(*) as job_count
     , cast ('' as char(1)) as dim_nm
     , cast ('' as char(1)) as dim_val
from da_views.dw_infra_etl_log
where uow_from > 0
and start_date between current_date - 60 and current_date - 1
group by 1,2,3,4,6,7

order by 1
