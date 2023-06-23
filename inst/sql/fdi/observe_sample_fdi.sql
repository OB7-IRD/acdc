-------------------------------------------------------------------------------
-- SAMPLE - PURSE SEINE - OBSERVE
-------------------------------------------------------------------------------
-- Extraction of samples from Observe v9 for FDI data call
-------------------------------------------------------------------------------
-- Philippe Sabarros <philippe.sabarros@ird.fr>
-------------------------------------------------------------------------------
-- 2023-06-16 -- v1.0 -- PS -- initial version
-------------------------------------------------------------------------------

SELECT
p.label1::text AS program
,EXTRACT(YEAR from r.date)::integer AS year
,o.label1::text AS ocean
,co.iso3code::text AS country
,v.label1::text AS vessel
,t.homeid::text AS home_id
,t.startdate::date AS trip_start_date
,t.enddate::date AS trip_end_date
,ob.lastname::text as observer
,r.date::date AS observation_date
,a.time::time AS observation_time
,a.latitude::numeric AS latitude
,a.longitude::numeric AS longitude
,(CASE WHEN st.code='0' THEN 'UNK' ELSE (CASE WHEN st.code='1' THEN 'FOB' ELSE (CASE WHEN st.code='2' THEN 'FSC' END) END) END)::text AS school_type
,sg.label1::text AS species_group
,sp.faocode::text AS fao_code
,sp.label1::text AS common_name
,sp.scientificlabel::text AS scientific_name
,sm.count::integer AS count
,smt.code::text AS length_type
,sm.length::numeric AS length
,sm.islengthcomputed::text AS length_was_computed
,sm.weight::numeric AS weight
,sm.isweightcomputed::text AS weight_was_computed
,sx.label1::text AS sex
,sf.label1::text AS fate
,sf.code::integer AS fate_code
,sa.comment::text AS sample_comment
,t.topiaid::text AS trip_id
,s.topiaid::text AS set_id
,sa.topiaid::text AS sample_id
,sm.topiaid::text AS samplemeasure_id

FROM ps_common.trip t
INNER JOIN ps_common.program p ON (t.observationsprogram = p.topiaid)
INNER JOIN common.ocean o ON (t.ocean = o.topiaid)
INNER JOIN common.person ob ON (t.observer = ob.topiaid)
INNER JOIN common.vessel v ON (t.vessel = v.topiaid)
INNER JOIN common.country co ON (v.flagcountry = co.topiaid)
INNER JOIN ps_observation.route r ON (r.trip = t.topiaid)
INNER JOIN ps_observation.activity a ON (a.route = r.topiaid)
INNER JOIN ps_common.vesselactivity va ON (a.vesselactivity = va.topiaid)
INNER JOIN ps_observation.set s ON (s.activity = a.topiaid)
INNER JOIN ps_observation.sample sa ON (sa.set = s.topiaid)
INNER JOIN ps_observation.samplemeasure sm ON (sm.sample = sa.topiaid)
INNER JOIN common.sizemeasuretype smt ON (sm.sizemeasuretype = smt.topiaid)
INNER JOIN common.species sp ON (sm.species = sp.topiaid) 
INNER JOIN common.speciesgroup sg ON (sp.speciesgroup = sg.topiaid)
LEFT OUTER JOIN common.sex sx ON (sm.sex = sx.topiaid)
LEFT OUTER JOIN ps_common.speciesfate sf ON (sm.speciesfate = sf.topiaid)
LEFT OUTER JOIN ps_common.schooltype st ON (s.schooltype = st.topiaid)

WHERE
sm.length IS NOT NULL
AND EXTRACT(YEAR from r.date) BETWEEN (?start_year) AND (?end_year)
AND p.topiaid IN (?program)
AND o.label1 IN (?ocean)
AND co.code IN (?flag)

ORDER BY
sp.faocode,
smt.code,
sm.length

;
