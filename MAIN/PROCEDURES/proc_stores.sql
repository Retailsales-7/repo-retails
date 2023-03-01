CREATE OR REPLACE PROCEDURE DB_RETAIL.MAIN.MYPROC1()
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS OWNER
AS '
def run(new_session):
  df_data1 = new_session.table(''DB_RETAIL.STG.STORE_CITIES'')
  VIS_STORE=df_data1.to_pandas() 
  VIS_STORE=VIS_STORE.astype({''STORE_SIZE'':int})
  
  final=new_session.create_dataframe(VIS_STORE)
  
  final.write.mode("overwrite").save_as_table(''DB_RETAIL.MAIN.VIS_STORE'')
  return "SUCCESS"
';