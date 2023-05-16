-- valor promedio, promedio de tamaño máximo de vela, porentaje representativo de la vela con el valor promedio
SELECT A.ticker, round(avg(H.closing_value), 2) as Avg_Value, round(avg(H.max_value - H.min_value), 2) as Avg_candle_size,
round(avg(H.max_value - H.min_value) / avg(H.closing_value) *100, 2) as Candle_perc, count(*) as rounds
FROM History H 
	JOIN Assets A on H.asset_id = A.id
WHERE H.closing_value >= H.opening_value
group by A.ticker