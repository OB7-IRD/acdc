select
	p.c_pays_fao::text as landing_country_fao
	,pa.c_pays_fao::text as vessel_fleet_country_fao
	,c.d_act::date as landing_date
	,a.v_la_act::numeric as latitude_dec
	,a.v_lo_act::numeric as longitude_dec
	,c.c_esp::integer as specie_code
	,e.l_esp_s ::text as specie_scientific_name
	,e.c_esp_fao::text as "CLspecFAO"
	,tb.c_engin::integer as vessel_type
	,c.v_poids_capt::numeric as "CLsciWeight"
	,p.c_locode::text as "CLloc"
	,b.v_l_ht::numeric as vessel_length
from 
	public.capture c
	join public.activite a on (c.c_bat = a.c_bat and c.d_act = a.d_act and c.n_act = a.n_act)
	join public.port p on (a.c_port = p.c_port)
	join public.bateau b on (a.c_bat = b.c_bat)
	join public.pavillon pa on (b.c_flotte = pa.c_pav_b)
	join public.espece e on (c.c_esp = e.c_esp)
	join public.type_bateau tb on (b.c_typ_b = tb.c_typ_b)
where 
	extract (year from a.d_dbq) in (?year_time_period)
	and b.c_flotte in (?fleet)
;
