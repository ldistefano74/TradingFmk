-- select asset_id, min(date), max(date) from history group by asset_id

select * from history where date between '2021-05-04' and '2021-05-10'
select * From assets
insert into assets (ticker, name, ppi_id) values ('JPM', 'J.P. MORGAN & CHASE CO.', 16031)
select * from intra where datetime between '2021-05-26' and '2021-05-27' and asset_id = 70 order by datetime

select max_value - min_value from history where asset_id = 70 and date between '20210426' and '20210602' 
order by max_value - min_value desc
select avg(max_value - min_value) from history where asset_id = 70 and date between '20210426' and '20210602' 
order by max_value - min_value desc

select avg((max_value - min_value) / opening_value * 100) from history where asset_id = 70 and date between '20210426' and '20210602'
	select avg((max_value - min_value) / opening_value * 100) 
	from history 
	where asset_id = 70 
		and date between '20210101' and '20210401'
		and opening_value > closing_value

SELECT * from intra where asset_id = 70 order by datetime 
SELECT avg(closing_value) * .439 from history where asset_id = 70 and date between '20201001' and '20201231'

select avg(closing_value - min_value), avg(closing_value), min(closing_value - min_value), max(closing_value - min_value)
from history
where closing_value < opening_value 
	and asset_id = 70 
	and date between '20210101' and '20210426'

select round(avg(closing_value - min_value), 2), round(avg(closing_value),2), round(avg(closing_value - min_value)/ avg(closing_value) * 100.0, 2), 
	min(closing_value - min_value), max(closing_value - min_value)
from history
where closing_value < opening_value 
	and asset_id = 70 
	and date > '20210724'

select A.ticker, round(avg(closing_value - opening_value ), 2), round(avg(max_value - opening_value ), 2), round(avg(opening_value), 2), 
	round(avg((closing_value - opening_value) / opening_value) * 100.0, 2) as promedio_positivo,
	round(avg((max_value - opening_value) / closing_value) * 100.0, 2) as pp_maximo
from history H
	join assets A on H.asset_id = A.id
where closing_value > opening_value and date > '20210601' --	and asset_id = 70
group by A.ticker
order by pp_maximo desc

select * from assets
select min(date), max(date) from history where asset_id = 6


SELECT 
-- analizar los mínimos durante los primeros minutos de apertura
select * from intra where asset_id = 70 and date(datetime) = '20210719' order by datetime
select max(date) from history where asset_id = 6

SELECT substr(ticker, 2), * FROM assets where id in (70, 71)
SELECT * FROM ASSETS WHERE TICKER IN ('ARCO', 'JPM')
DELETE FROM ASSETS WHERE ID IN (295, 388)
update assets set ticker = substr(ticker, 2) where id in (70, 71)


select round()
from History