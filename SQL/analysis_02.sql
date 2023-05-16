/*
-- borrar duplicados en Intra
DELETE from intra where id in(
-- select datetime, value, count(*) as cant, min(id) min_id
select min(id) min_id
FROM Intra 
group by datetime, value
having count(*) > 1
ORDER BY datetime
)
*/

/*
-- Chequear si los mínimos de history son los que aparecen en intra
select date(datetime), count(*), min(I.value), max(I.value), H.Min_Value, H.Max_value
FROM Intra I
	JOIN History H on I.Asset_id = H.Asset_Id and date(I.datetime) = H.Date
group by date(I.datetime), H.Min_Value, H.Max_value
*/


drop table if exists tmp_Assets;
drop table if exists tmp_MinMax;

select Asset_Id, count(*) as rounds 
into temp tmp_Assets 
from Assets A
	join History H on A.Id = H.Asset_id
where Ticker in ('BBAR')
group by Asset_Id;

select A.Asset_Id, H.Date, extract( hour from min(I.DateTime)) min_Hour, extract( minute from min(I.DateTime)) min_Minutes, 
	min_value, extract( hour from max(M.DateTime)) max_Hour, extract( minute from max(M.DateTime)) max_Minutes, max_value
into temp tmp_MinMax
from tmp_Assets A
	join history H on A.Asset_Id = H.Asset_id
	join Intra I on H.Date = date(I.datetime) and I.value = H.min_value
	join Intra M on H.Date = date(M.datetime) and M.value = H.max_value
group by A.Asset_Id, H.Date, H.min_value, H.max_Value
order by A.Asset_Id, H.Date;

select A.Asset_Id, MI.hour, tot_min, tot_max, round(tot_min::decimal/A.rounds, 2) as min_ratio, round(tot_max::decimal/A.rounds, 2) as max_ratio
from 
	tmp_Assets A 
	join (select Asset_Id, Min_hour as Hour, count(*) as tot_min from tmp_MinMax group by Asset_Id, Min_Hour) MI on A.asset_id = MI.asset_id
	join (select Asset_Id, Max_hour as Hour, count(*) as tot_max from tmp_MinMax group by Asset_Id, Max_Hour) MA on A.asset_id = MA.asset_id and MI.Hour = MA.Hour
order by A.Asset_Id, MI.Hour

-- Los mínimos se dan en los primeros minutos del día
/*
select Asset_Id, Min_hour, min_minutes, count(*) as tot_min 
from tmp_MinMax 
group by Asset_Id, Min_Hour, min_minutes
Order by tot_min desc

select Asset_Id, Max_hour, max_minutes, count(*) as tot_max
from tmp_MinMax 
group by Asset_Id, max_Hour, max_minutes
Order by tot_max desc
*/