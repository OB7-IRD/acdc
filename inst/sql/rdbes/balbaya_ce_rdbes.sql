select 
	pa.c_pays_fao::text as vessel_fleet_country_fao
	,a.d_dbq::date as landing_date
	,a.v_la_act::numeric as latitude_decimal
	,a.v_lo_act::numeric as longitude_decimal
	,b.c_bat::integer as vessel_code
	,b.c_typ_b::integer as vessel_type_code
	,tb.l_typ_b::text as vessel_type_label
	,p.c_locode::text as "CEloc"
	,b.v_l_ht::numeric as vessel_length
	,a.v_tmer::numeric AS "CEoffVesHoursAtSea"
	,a.v_tpec::numeric AS fishing_time_hours
	,b.v_p_cv::numeric AS vessel_engine_power
	,b.v_ct_m3::numeric AS vessel_volume
	,a.c_ocea::integer AS ocean
from
	public.maree m 
	join public.activite a on (a.c_bat = m.c_bat and a.d_dbq = m.d_dbq)
	join public.bateau b on (a.c_bat = b.c_bat)
	join public.pavillon pa on (b.c_flotte = pa.c_pav_b)
	join public.type_bateau tb on (b.c_typ_b = tb.c_typ_b)
	join public.port p on (a.c_port = p.c_port)
where 
	extract (year from a.d_dbq) in (?year_time_period)
	and b.c_flotte in (?fleet)
;

	
	
