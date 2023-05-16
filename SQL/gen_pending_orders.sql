drop table if exists temp_dates;
drop table if exists temp_dif;
-- select current_date as rdate into temp temp_dates;
-- select date(sdate) as rdate into temp temp_dates from generate_series('20220214', current_date, interval '1' day) as t(sdate);
select date(sdate) as rdate into temp temp_dates from generate_series('20220214', '20220628', interval '1' day) as t(sdate);

-- ordenes no cerradas
select date(datetime) as date, asset_id, sum(done_nominals * (case when order_type = 'BUY' then 1 else -1 end)) as done,
	strategy_id
into temp temp_dif
from orders 
where date(datetime) in (select rdate from temp_dates)
group by date(datetime), asset_id, strategy_id
having sum(done_nominals * (case when order_type = 'BUY' then 1 else -1 end)) > 0
order by asset_id;

/*
insert into orders (datetime, asset_id, order_type, order_status, price, nominals, done_nominals, strategy_id, order_number)
(
	select D.date+'17:00:00'::time as datetime, D.asset_id, 'SELL' as order_type, 'COMPLETED' as order_status, 
		H.closing_value as price, 
		D.Done as nominals, D.done as done_nominals, D.strategy_id, 'GENERADA X DIF' as order_number
	from temp_dif D
		join history H on H.date = D.date and H.asset_id = D.asset_id
	order by D.asset_id, D.date
)
*/

