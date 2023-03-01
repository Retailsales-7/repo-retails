create or replace task DB_RETAIL.MAIN.T1
	warehouse=RETAIL_WHS
	schedule='1 MINUTE'
	as call MYPROC3();