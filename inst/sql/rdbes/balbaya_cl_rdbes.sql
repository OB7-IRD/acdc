select
	p.c_pays_fao::text as landing_country_fao
	,pa.c_pays_fao::text as vessel_flag_country_fao
	,a.d_dbq::date as landing_date
	,a.v_la_act::numeric as latitude_decimal
	,a.v_lo_act::numeric as longitude_decimal
	,c.c_esp::integer as specie_code
	,e.l_esp::text as specie_label
	,e.c_esp_fao::text as "CLspecFAO"
	,e.l_esp_s ::text as specie_scientific_name
	,b.c_bat::integer as vessel_code
	,b.c_typ_b::integer as vessel_type_code
	,tb.l_typ_b::text as vessel_type_label
	,c.v_poids_capt::numeric as "CLsciWeight"
	,p.c_locode::text as "CLloc"
	,b.v_l_ht::numeric as vessel_length
from 
	public.capture c
	join public.activite a on (c.c_bat = a.c_bat and c.d_act = a.d_act and c.n_act = a.n_act)
	join public.port p on (a.c_port = p.c_port)
	join public.bateau b on (a.c_bat = b.c_bat)
	join public.pavillon pa on (b.c_pav_b  = pa.c_pav_b)
	join public.espece e on (c.c_esp = e.c_esp)
	join public.type_bateau tb on (b.c_typ_b = tb.c_typ_b)
where 
	extract (year from a.d_dbq) in (?year_time_period)
	and b.c_pav_b in (?flag)
;
