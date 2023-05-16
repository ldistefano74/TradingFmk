-- analizar el mínimo/apertura del día vs el cierre anterior: devuelve el promedio de diferencia (en %) entre el mínimo del día y el cierre anterior. Por ej  para ARCO, en promedio, el mínimo es un 2% mas bajo que el cierre del día anterior
drop table if exists temp_Assets;
drop table if exists temp_P;
drop table if exists temp_x;

select id  
into temp temp_assets 
from assets where ticker in ('ARCO','MELI') or true;

select H.*, row_number() over (order by H.Asset_id, H.date) as Orden
into temp temp_P
From history H 
where asset_id IN (select Id from Temp_Assets)
order by asset_id, date;

/*
select H.Asset_id, H.Date, H.Closing_Value, H.Opening_value, H.Min_Value, H.Max_Value, P.prev_closing, (round(H.Opening_value/P.Prev_closing, 2)-1) * 100 as porc_inicio,
	(round(h.min_value / p.prev_closing, 2) - 1) * 100 as porc_min
into temp temp_X
From Temp_P H
	left join (select asset_id, closing_value as prev_closing, row_number() over (order by date) + 1 as orden 
			   from History where Asset_id IN (select Id from Temp_Assets) order by asset_id, Date) P on H.asset_id = P.asset_id and H.Orden = P.Orden
where H.asset_id IN (select Id from Temp_Assets)
order by H.asset_id, H.date;
*/

select H.Asset_id, H.Date, H.Closing_Value, H.Opening_value, H.Min_Value, H.Max_Value, P.prev_closing, H.orden as orden1, P.Orden as orden2,
	round((H.Opening_value / P.Prev_closing - 1) * 100, 2) as porc_inicio,
	round((h.min_value / p.prev_closing - 1) * 100, 2) as porc_min
into temp temp_X
From Temp_P H
	left join (select asset_id, closing_value as prev_closing, orden + 1 as orden 
			   from temp_P order by asset_id, date) P on H.asset_id = P.asset_id and H.Orden = P.Orden
order by H.asset_id, H.date;

--select * from temp_x;

select A.ticker, round(avg(porc_min), 2) avg_min_close
from temp_x X
	join Assets A on X.asset_id = A.id
group by A.ticker
order by avg_min_close


