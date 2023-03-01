CREATE OR REPLACE PROCEDURE DB_RETAIL.MAIN.MYPROC2()
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS OWNER
AS '
def run(new_session): 
  df_data1 = new_session.table(''STG."PRODUCT_HIERARCHY"'')
  VIS_PRODUCT=df_data1.to_pandas() 
  VIS_PRODUCT=VIS_PRODUCT.astype({''PRODUCT_LENGTH'':float,''PRODUCT_DEPTH'':float,''PRODUCT_WIDTH'':float})
  VIS_PRODUCT["PRODUCT_LENGTH"] = VIS_PRODUCT["PRODUCT_LENGTH"].fillna(0.0)
  VIS_PRODUCT["PRODUCT_DEPTH"] = VIS_PRODUCT["PRODUCT_DEPTH"].fillna(0.0)
  VIS_PRODUCT["PRODUCT_WIDTH"] = VIS_PRODUCT["PRODUCT_WIDTH"].fillna(0.0)
  VIS_PRODUCT["CLUSTER_ID"] = VIS_PRODUCT["CLUSTER_ID"].fillna("NONE")
   
  
  final=new_session.create_dataframe(VIS_PRODUCT)
  
  final.write.mode("overwrite").save_as_table(''DB_RETAIL.MAIN.VIS_PRODUCT'')
  return "SUCCESS"
';