select 
	v.topiaid::text as vessel_id
	,v.code::integer as vessel_code
	,v.label1::text as vessel_name
from 
	common.vessel v 
;
