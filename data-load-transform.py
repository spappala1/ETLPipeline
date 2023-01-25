# Databricks notebook source
# MAGIC %md
# MAGIC #### Python script performs the below tasks:
# MAGIC * Ingests the data from csv file and creates dataframe
# MAGIC * Transforms the data (Change "id" datatype column to int)
# MAGIC * Creates database in lakehouse
# MAGIC * Creates database tables and loads data

# COMMAND ----------

# MAGIC %md
# MAGIC #### Parameters:
# MAGIC * **filename** - This represents the filename which is the source datasource
# MAGIC * **databasename** - Name of the database that need to be created for storing the data
# MAGIC * **tablename** - Table name inside the database that need to created for loading the data from dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Data Extraction

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Define the widgets to enter the values of the parameters

# COMMAND ----------

#dbutils.widgets.remove("TempTableName")
dbutils.widgets.text("filename", "SRDataEngineerChallenge_DATASET.csv", "Enter file name in csv format")
dbutils.widgets.text("databasename", "sfldatabase", "Enter database name")
dbutils.widgets.text("tablename", "sfltable", "Enter Table name")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Assign parameter values to variables

# COMMAND ----------

import pandas as pd
filename = dbutils.widgets.get("filename")
databasename = dbutils.widgets.get("databasename")
tablename = dbutils.widgets.get("tablename")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Read the csv file and create pandas dataframe

# COMMAND ----------

# Read all as String
hrdata = pd.read_csv(filename,converters={i: str for i in range(100)}) 

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Data Transformation

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Convert column datatype to int

# COMMAND ----------

hrdata['id'] = hrdata['id'].str.replace("\$|,", "").astype(int)


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Convert pandas dataframe to spark dataframe

# COMMAND ----------

# From pandas to DataFrame
df_hrdata = sqlContext.createDataFrame(hrdata)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Data Loading

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Define source, database name and table name

# COMMAND ----------

username = spark.sql("SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]
source = f"dbfs:/user/{username}/copy-into-demo"
spark.sql(f"SET c.username='{username}'")
spark.sql(f"SET c.databasename={databasename}")
spark.sql(f"SET c.source='{source}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Database Creation
# MAGIC * Drop database if existing with same name
# MAGIC * Create database
# MAGIC * Define to use the created database

# COMMAND ----------

spark.sql("DROP DATABASE IF EXISTS ${c.databasename} CASCADE")
spark.sql("CREATE DATABASE ${c.databasename}")
spark.sql("USE ${c.databasename}")

dbutils.fs.rm(source, True)
          
          


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Write/Load the data from dataframe to table 
# MAGIC * Drop the table if existing with the same name

# COMMAND ----------

# Write the data to a table.
spark.sql("DROP TABLE IF EXISTS " + tablename)
df_hrdata.write.saveAsTable(tablename)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Query the data from the table

# COMMAND ----------

spark.sql("select * from " + tablename).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Conclusion
# MAGIC We have successfully extracted data from csv file, transformed and loaded it into the **database table** in databricks **data lakehouse** 
