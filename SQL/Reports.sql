drop table if exists temp_dates;
-- select current_date as rdate into temp temp_dates;
-- select date(sdate) as rdate into temp temp_dates from generate_series('20220214', current_date, interval '1' day) as t(sdate);
select date(sdate) as rdate into temp temp_dates from generate_series('20220214', '20220614', interval '1' day) as t(sdate);

select * 
from orders 
where date(datetime) in (select rdate from temp_dates)
order by id;

-- Resultados por strategy
select strategy_id, sum(done_nominals * (case when order_type = 'BUY' then 1 else 0 end)) nominales, 
	sum(done_nominals * price * (case when order_type = 'BUY' then -1 else 1 end)) as resultado
from orders 
where date(datetime) in (select rdate from temp_dates)
group by strategy_id
order by resultado desc;

-- Balance total e inversión requerida
select inversion, resultado, round(resultado / inversion * 100, 2) as p_resultado
from (
	select sum(done_nominals * price * (case when order_type = 'BUY' then -1 else 1 end)) as resultado,
		sum(done_nominals * price * (case when order_type = 'BUY' then 1 else 0 end)) as inversion
	from orders 
	where date(datetime) in (select rdate from temp_dates)
) R;



select strategy_id, sum(done_nominals * (case when order_type = 'BUY' then -1 else 1 end)) as done
from orders 
where date(datetime) in (select rdate from temp_dates)
group by strategy_id
order by strategy_id;


-- ordenes no cerradas
select date(datetime), asset_id, sum(done_nominals * (case when order_type = 'BUY' then 1 else -1 end)) as done
from orders 
where date(datetime) in (select rdate from temp_dates)
group by date(datetime), asset_id
having sum(done_nominals * (case when order_type = 'BUY' then 1 else -1 end)) > 0
order by asset_id;


select date(datetime) as date, strategy_id, sum(done_nominals * (case when order_type = 'BUY' then 1 else 0 end)) nominales, 
	sum(done_nominals * price * (case when order_type = 'BUY' then -1 else 1 end)) as resultado
from orders 
where date(datetime) in (select rdate from temp_dates) and strategy_id = 'FB_1'
group by date, strategy_id
order by resultado desc;
