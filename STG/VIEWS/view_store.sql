create or replace view DB_RETAIL.STG.VIEW_STORE_CITIES(
	STORE_ID,
	STORETYPE_ID,
	STORE_SIZE,
	CITY_ID
) as select * from DB_RETAIL.STG.STORE_CITIES;