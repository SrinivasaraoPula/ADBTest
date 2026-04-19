# Databricks notebook source
# MAGIC %md
# MAGIC Datab Reading

# COMMAND ----------

df=spark.read.format('csv').option('header',True).option('inferschema',True).load('/Volumes/workspace/default/tutorial/BigMart Sales.csv')

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Data Reading json 

# COMMAND ----------

df_json=spark.read.format('json')\
    .option("header",True)\
        .option("inferSchema",True)\
        .load('/Volumes/workspace/default/tutorial/drivers.json')

# COMMAND ----------

df_json.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

my_ddl_schema='''
Item_Identifier string,
Item_Weight string,
Item_Fat_Content string,
Item_Visibility double,
Item_Type string,
Item_MRP double,
Outlet_Identifier string,
Outlet_Establishment_Year integer,
Outlet_Size string,
Outlet_Location_Type string,
Outlet_Type string,
Item_Outlet_Sales double
'''

# COMMAND ----------

df=spark.read.format('csv').option('header',True).schema(my_ddl_schema).load('/Volumes/workspace/default/tutorial/BigMart Sales.csv')

# COMMAND ----------

df.display()

# COMMAND ----------

