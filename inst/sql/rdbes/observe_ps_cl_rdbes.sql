with harbour_country_fao as (
	select
		h.topiaid::text as harbour_topiaid
		,co.iso3code::text as harbour_country_fao
		,h.locode::text as harbour_locode
	from
		common.harbour h
		join common.country co on (h.country = co.topiaid))
select
	hcf.harbour_country_fao::text as landing_country_fao
	,co.iso3code::text as vessel_fleet_country_fao
	,t.enddate::date as landing_date
	,a.latitude::numeric as latitude_decimal
	,a.longitude::numeric as longitude_decimal
	,s.code::integer as specie_code
	,s.faocode::text as "CLspecFAO"
	,s.scientificlabel::text as specie_scientific_name
	,sf.code::integer as specie_fate_code
	,sf.label1::text as specie_fate_label
	,v.topiaid::text as "CLencrypVesIds"
	,v.code::integer as vessel_code
	,vt.code::integer as vessel_type_code
	,vt.label2::text as vessel_type_label
	,c.weight::numeric as "CLoffWeight"
	,hcf.harbour_locode::text as "CLloc"
	,v.length::numeric as vessel_length
from
	ps_logbook.catch c
	join ps_logbook.activity a on (c.activity = a.topiaid)
	join ps_logbook.route r on (a.route = r.topiaid)
	join ps_common.trip t on (r.trip = t.topiaid)
	join common.vessel v on (t.vessel = v.topiaid)
	join common.country co on (v.fleetcountry = co.topiaid)
	join harbour_country_fao hcf on (t.landingharbour = hcf.harbour_topiaid)
	join common.species s on (c.species = s.topiaid)
	join common.vesseltype vt on (v.vesseltype = vt.topiaid)
	join ps_common.speciesfate sf on (c.speciesfate = sf.topiaid)
where
	extract (year from t.enddate) in (?year_time_period)
	and co.code in (?fleet)
;
