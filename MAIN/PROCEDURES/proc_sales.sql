CREATE OR REPLACE PROCEDURE DB_RETAIL.MAIN.MYPROC3()
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS OWNER
AS '
def run(new_session):
  import pandas as pd
  df_data1 = new_session.table(''DB_RETAIL.STG.SALES'')
  VIS_SALES=df_data1.to_pandas() 
  VIS_SALES[''YEAR''] = pd.DatetimeIndex(VIS_SALES[''DATE'']).year
  VIS_SALES[''MONTH''] = pd.DatetimeIndex(VIS_SALES[''DATE'']).month
  VIS_SALES[''DAY''] = pd.DatetimeIndex(VIS_SALES[''DATE'']).day
  VIS_SALES[''QUARTER''] = pd.DatetimeIndex(VIS_SALES[''DATE'']).quarter
  VIS_SALES=VIS_SALES.astype({''SALES'':float,''PRICE'':float})
  VIS_SALES[''SALES_PRICE'']=VIS_SALES[''SALES'']*VIS_SALES[''PRICE'']
  VIS_SALES["SALES"] = VIS_SALES["SALES"].fillna(0.0)
  VIS_SALES=VIS_SALES.astype({''REVENUE'':float,''STOCK'':float,''PROMO_DISCOUNT_2'':float})
  VIS_SALES["REVENUE"] = VIS_SALES["REVENUE"].fillna(0.0)
  VIS_SALES["STOCK"] = VIS_SALES["STOCK"].fillna(0.0)
  VIS_SALES["PROMO_DISCOUNT_2"] = VIS_SALES["PROMO_DISCOUNT_2"].fillna(0.0)
  VIS_SALES["PRICE"] = VIS_SALES["PRICE"].fillna(0.0)
  VIS_SALES["PROMO_BIN_1"] = VIS_SALES["PROMO_BIN_1"].fillna(''NONE'')
  VIS_SALES["PROMO_BIN_2"] = VIS_SALES["PROMO_BIN_2"].fillna(''NONE'')
  VIS_SALES["SALES_PRICE"] = VIS_SALES["SALES_PRICE"].fillna(0.0)
  VIS_SALES["PROMO_DISCOUNT_TYPE_2"] = VIS_SALES["PROMO_DISCOUNT_TYPE_2"].fillna(''NONE'')
  final=new_session.create_dataframe(VIS_SALES)
  final.write.mode("overwrite").save_as_table(''DB_RETAIL.MAIN.VIS_SALES'')
  return "SUCCESS"
';