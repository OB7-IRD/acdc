select 
	v.topiaid::text as "VDencrVessCode"
	,v.code::integer as vessel_code
	,v.label1::text as vessel_name
	,v.yearservice::integer as year_service
	,co.iso3code::text as vessel_flag_country_fao
	,co.label1::text as vessel_flag_country_fao_name
	,v.length::numeric as "VDlen"
	,v.powercv::numeric as engine_power_cv
	,v.capacity::numeric as capacity_m3
from 
	common.vessel v
	join common.country co on (v.flagcountry  = co.topiaid)
where
	v.status = 1
	and co.code in (?flag)
;
