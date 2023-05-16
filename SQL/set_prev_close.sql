CREATE OR REPLACE PROCEDURE set_prev_close (asset_ticker varchar = null)
LANGUAGE plpgsql 
AS $$
declare
	_asset_id integer := null;
begin
	if asset_ticker is not null then
		select id into _asset_id from assets where ticker = asset_ticker;
	end if;

	create temp table temp_p on commit drop as
	select H.*, row_number() over (order by H.Asset_id, H.date) as Orden
	From history H 
	where _asset_id is null or asset_id = _asset_id
	order by asset_id, date;

	create temp table temp_x on commit drop as
	select H.*, P.prev_closing
	From temp_p H
		left join (select asset_id, closing_value as prev_closing, row_number() over (order by asset_id, date) + 1 as orden 
				   from History where _asset_id is null or asset_id = _asset_id order by asset_id, Date) P on H.asset_id = P.asset_id and H.Orden = P.Orden
	order by H.asset_id, H.date;

	update history H 
	set prev_close = X.prev_closing
	from temp_x X
	where H.id = X.id;

	--COMMIT;
	DROP TABLE IF EXISTS temp_p;
	DROP TABLE IF EXISTS temp_x;
end
$$