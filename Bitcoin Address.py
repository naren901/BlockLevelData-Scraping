# Databricks notebook source
sc.version

# COMMAND ----------

#install required python libraries and load them
#%run ./install_python_package
%pip install bs4
%pip install requests
%pip install wget
%pip install pyspark

# COMMAND ----------

import requests
import zipfile
import requests
from os.path import basename
import gzip
import shutil
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import dataframe


# COMMAND ----------

# MAGIC %md  ### Steps
# MAGIC   1) Create download folders in DRIVER (Common name for all coins) <br/>
# MAGIC   2) Creates coin specific folder structure in MOUNT  <br/>
# MAGIC   3) Gets the list of zip file download urls for coin type  <br/>
# MAGIC   4) Execute download (Downloaded to DRIVER)  <br/>
# MAGIC   5) Extract the zip files into TSV files (in DRIVER)  <br/>
# MAGIC   6) Move downloaded files, Extracts to coin specific folders in Mount  <br/>
# MAGIC   7) Load TSV files into dataframe  <br/>
# MAGIC   8) Save Selected columns from Dataframe into a CSV file  <br/>
# MAGIC   3) Copy CSV file to FILESTORE to download using Web Browser  <br/>
# MAGIC   

# COMMAND ----------

# MAGIC %md ### Common Methods
# MAGIC These methods do not depend on coin specific folder structure, download things to DRIVER <br/>
# MAGIC 1) Create download folders in Driver (Common name for all coins) <br/>
# MAGIC 3) Gets the list of zip file download urls for coin type <br/>
# MAGIC 4) Execute download (Downloaded to Driver) <br/>
# MAGIC 5) Extract the zip files into TSV files (in Driver) <br/>

# COMMAND ----------

def create_driver_folders():
  dbutils.fs.mkdirs("file:/databricks/driver/ZipDownloads")
  dbutils.fs.mkdirs("file:/databricks/driver/ZipDownloads/Extracts")

# COMMAND ----------

def cleanup_driver_location():
  dbutils.fs.rm("file:/databricks/driver/ZipDownloads/",  True)
  dbutils.fs.rm("file:/databricks/driver/ZipDownloads/Extracts/",  True)

# COMMAND ----------

# MAGIC %md ### (1) Address data

# COMMAND ----------

address_file_urls = [
["https://gz.blockchair.com/bitcoin/addresses/blockchair_bitcoin_addresses_latest.tsv.gz?key=202001ZjMvj8R3BF", "BITCOIN"],
["https://gz.blockchair.com/bitcoin-cash/addresses/blockchair_bitcoin-cash_addresses_latest.tsv.gz?key=202001ZjMvj8R3BF","BITCOIN-CASH"],
["https://gz.blockchair.com/bitcoin-sv/addresses/blockchair_bitcoin-sv_addresses_latest.tsv.gz?key=202001ZjMvj8R3BF", "BITCOIN-SV"],
["https://gz.blockchair.com/dogecoin/addresses/blockchair_dogecoin_addresses_latest.tsv.gz?key=202001ZjMvj8R3BF", "DOGECOIN"],
["https://gz.blockchair.com/dash/addresses/blockchair_dash_addresses_latest.tsv.gz?key=202001ZjMvj8R3BF", "DASH"],
["https://gz.blockchair.com/litecoin/addresses/blockchair_litecoin_addresses_latest.tsv.gz?key=202001ZjMvj8R3BF", "LITECOIN"]
]

# COMMAND ----------

# MAGIC %md
# MAGIC Create download folders and Folders on Mount

# COMMAND ----------

def create_folder_structure():
  cleanup_driver_location()
  create_driver_folders() 
  dbutils.fs.rm("dbfs:/mnt/BlockChain/Address/TSVData", True)
  dbutils.fs.rm("dbfs:/mnt/BlockChain/Address/ZipFIles", True)
  dbutils.fs.mkdirs("dbfs:/mnt/BlockChain/Address/TSVData")
  dbutils.fs.mkdirs("dbfs:/mnt/BlockChain/Address/ZipFIles")

# COMMAND ----------

create_folder_structure()

# COMMAND ----------

# MAGIC %md
# MAGIC Download the address Zip Files

# COMMAND ----------

# MAGIC %md
# MAGIC Extract the donwloaded address Zip Files

# COMMAND ----------

# MAGIC %sh
# MAGIC wget "https://gz.blockchair.com/bitcoin/addresses/blockchair_bitcoin_addresses_latest.tsv.gz?key=202001ZjMvj8R3BF"

# COMMAND ----------

# MAGIC %sh
# MAGIC wget "https://gz.blockchair.com/bitcoin-cash/addresses/blockchair_bitcoin-cash_addresses_latest.tsv.gz?key=202001ZjMvj8R3BF"

# COMMAND ----------

# MAGIC %sh
# MAGIC wget "https://gz.blockchair.com/bitcoin-sv/addresses/blockchair_bitcoin-sv_addresses_latest.tsv.gz?key=202001ZjMvj8R3BF"

# COMMAND ----------

# MAGIC %sh
# MAGIC wget "https://gz.blockchair.com/dogecoin/addresses/blockchair_dogecoin_addresses_latest.tsv.gz?key=202001ZjMvj8R3BF"

# COMMAND ----------

# MAGIC %sh
# MAGIC wget "https://gz.blockchair.com/dash/addresses/blockchair_dash_addresses_latest.tsv.gz?key=202001ZjMvj8R3BF"

# COMMAND ----------

# MAGIC %sh
# MAGIC wget "https://gz.blockchair.com/litecoin/addresses/blockchair_litecoin_addresses_latest.tsv.gz?key=202001ZjMvj8R3BF"

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -all  /databricks/driver

# COMMAND ----------

dbutils.fs.mv("file:/databricks/driver/blockchair_bitcoin-cash_addresses_latest.tsv", "dbfs:/mnt/BlockChain/Address/TSVData/blockchair_bitcoin-cash_addresses_latest.tsv")

# COMMAND ----------

dbutils.fs.mv("file:/databricks/driver/blockchair_bitcoin_addresses_latest.tsv", "dbfs:/mnt/BlockChain/Address/TSVData/blockchair_bitcoin_addresses_latest.tsv")

# COMMAND ----------

dbutils.fs.mv("file:/databricks/driver/blockchair_bitcoin_sv_addresses_latest_tsv", "dbfs:/mnt/BlockChain/Address/TSVData/blockchair_bitcoin_sv_addresses_latest_tsv")

# COMMAND ----------

dbutils.fs.mv("file:/databricks/driver/blockchair_dash_addresses_latest__1__tsv", "dbfs:/mnt/BlockChain/Address/TSVData/blockchair_dash_addresses_latest__1__tsv")

# COMMAND ----------

dbutils.fs.mv("file:/databricks/driver/blockchair_litecoin_addresses_latest_tsv", "dbfs:/mnt/BlockChain/Address/TSVData/blockchair_litecoin_addresses_latest_tsv")

# COMMAND ----------

dbutils.fs.mv("file:/databricks/driver/blockchair_dogecoin_addresses_latest_tsv", "dbfs:/mnt/BlockChain/Address/TSVData/blockchair_dogecoin_addresses_latest_tsv")

# COMMAND ----------

import os  
os.rename('/databricks/driver/blockchair_bitcoin_addresses_latest.tsv.gz?key=202001ZjMvj8R3BF','/databricks/driver/blockchair_bitcoin_addresses_latest.tsv.gz')

# COMMAND ----------

dbutils.fs.cp("dbfs:/FileStore/tables/blockchair_bitcoin_sv_addresses_latest_tsv.gz", "file:/databricks/driver/")

# COMMAND ----------

dbutils.fs.cp("file:/databricks/driver/blockchair_bitcoin_addresses_latest.tsv.gz?key=202001ZjMvj8R3BF", "file:/databricks/blockchair_bitcoin_addresses_latest.tsv.gz")

# COMMAND ----------

dbutils.fs.mv("dbfs:/FileStore/tables/blockchair_dash_addresses_latest__1__tsv.gz", "file:/databricks/driver/blockchair_dash_addresses_latest__1__tsv.gz")

# COMMAND ----------

dbutils.fs.mv("dbfs:/FileStore/tables/blockchair_dogecoin_addresses_latest_tsv.gz", "file:/databricks/driver/blockchair_dogecoin_addresses_latest_tsv.gz")

# COMMAND ----------

dbutils.fs.mv("dbfs:/FileStore/tables/blockchair_litecoin_addresses_latest_tsv.gz", "file:/databricks/driver/blockchair_litecoin_abddresses_latest_tsv.gz")

# COMMAND ----------

# MAGIC %sh
# MAGIC gunzip /databricks/driver/blockchair_bitcoin_addresses_latest.tsv.gz

# COMMAND ----------

# MAGIC %md
# MAGIC Load into a dataframes

# COMMAND ----------

dbutils.fs.ls("dbfs:/mnt/BlockChain/Address/TSVData")

# COMMAND ----------

address_bitcoin_df = sqlContext.read.format('com.databricks.spark.csv').options(header='false', delimiter='\t', inferschema='true').load('dbfs:/mnt/BlockChain/Address/TSVData/blockchair_bitcoin_addresses_latest.tsv')

address_bitcoin_cash_df = sqlContext.read.format('com.databricks.spark.csv').options(header='false', delimiter='\t', inferschema='true').load('dbfs:/mnt/BlockChain/Address/TSVData/blockchair_bitcoin-cash_addresses_latest.tsv')

address_bitcoin_sv_df = sqlContext.read.format('com.databricks.spark.csv').options(header='false', delimiter='\t', inferschema='true').load('dbfs:/mnt/BlockChain/Address/TSVData/blockchair_bitcoin_sv_addresses_latest_tsv')

address_dash_df = sqlContext.read.format('com.databricks.spark.csv').options(header='false', delimiter='\t', inferschema='true').load('dbfs:/mnt/BlockChain/Address/TSVData/blockchair_dash_addresses_latest__1__tsv')

address_lite_coin_df = sqlContext.read.format('com.databricks.spark.csv').options(header='false', delimiter='\t', inferschema='true').load('dbfs:/mnt/BlockChain/Address/TSVData/blockchair_litecoin_addresses_latest_tsv')

address_dogecoin_cash_df = sqlContext.read.format('com.databricks.spark.csv').options(header='false', delimiter='\t', inferschema='true').load('dbfs:/mnt/BlockChain/Address/TSVData/blockchair_dogecoin_addresses_latest_tsv')

# COMMAND ----------

print (address_bitcoin_df.count() ,address_bitcoin_sv_df.count() ,address_dash_df.count() ,address_bitcoin_cash_df.count() , address_lite_coin_df.count(), address_dogecoin_cash_df.count())

# COMMAND ----------

address_bitcoin_cash_df.count()

# COMMAND ----------

from pyspark.sql.functions import lit
from pyspark.sql.functions import col

# COMMAND ----------

address_bitcoin_df = address_bitcoin_df.withColumnRenamed("_c0", "address").withColumnRenamed("_c1", "balance")
address_bitcoin_df = address_bitcoin_df.select(col("address"),col("balance"),lit("BitCoin").alias("Name"))

# COMMAND ----------

address_bitcoin_sv_df = address_bitcoin_sv_df.withColumnRenamed("_c0", "address").withColumnRenamed("_c1", "balance")
address_bitcoin_sv_df = address_bitcoin_sv_df.select(col("address"),col("balance"),lit("BitCoin-SV").alias("Name"))

# COMMAND ----------

address_dash_df = address_dash_df.withColumnRenamed("_c0", "address").withColumnRenamed("_c1", "balance")
address_dash_df = address_dash_df.select(col("address"),col("balance"),lit("Dash").alias("Name"))

# COMMAND ----------

address_lite_coin_df = address_lite_coin_df.withColumnRenamed("_c0", "address").withColumnRenamed("_c1", "balance")
address_lite_coin_df = address_lite_coin_df.select(col("address"),col("balance"),lit("LiteCoin-Cash").alias("Name"))

# COMMAND ----------

address_dogecoin_cash_df = address_dogecoin_cash_df.withColumnRenamed("_c0", "address").withColumnRenamed("_c1", "balance")
address_dogecoin_cash_df = address_dogecoin_cash_df.select(col("address"),col("balance"),lit("DogeCoin").alias("Name"))

# COMMAND ----------

address_bitcoin_cash_df = address_bitcoin_cash_df.withColumnRenamed("_c0", "address").withColumnRenamed("_c1", "balance")
address_bitcoin_cash_df = address_bitcoin_cash_df.select(col("address"),col("balance"),lit("BitCoin-Cash").alias("Name"))

# COMMAND ----------

from functools import reduce  # For Python 3.x
from pyspark.sql import DataFrame

# COMMAND ----------

new_merged_df_1 = address_bitcoin_df.union(address_bitcoin_sv_df)

# COMMAND ----------

new_merged_df_2 = address_dash_df.union(address_bitcoin_cash_df)

# COMMAND ----------

new_merged_df_3 = address_lite_coin_df.union(address_dogecoin_cash_df)

# COMMAND ----------

new_merged_df_2 = new_merged_df_2.union(new_merged_df_1)

# COMMAND ----------

new_merged_df_3 =new_merged_df_3.union(new_merged_df_2)

# COMMAND ----------

new_merged_df_3.count()

# COMMAND ----------

print ( address_bitcoin_df.count() ,address_bitcoin_sv_df.count() ,address_dash_df.count() ,address_bitcoin_cash_df.count() , address_lite_coin_df.count(), address_dogecoin_cash_df.count())
(12) Spark Jobs
37978757 19825369 1357077 16358274 3022507 3551936

# COMMAND ----------

new_merged_df_3.select("Name").distinct().collect()

# COMMAND ----------

new_merged_df_3[["address", "balance","Name"]].coalesce(1).write.option("header", "true").csv("dbfs:/mnt/BlockChain/Address/Merged_Address.csv")
 #dbutils.fs.cp("dbfs:/mnt/BlockChain/Blocks/Bitcoin-cash/bc_block_sub_df_exported.csv", "dbfs:/FileStore/bc_block_sub_df_exported.csv", recurse = True)

# COMMAND ----------

address_bitcoin_df[["address", "balance","Name"]].coalesce(1).write.option("header", "true").csv("dbfs:/mnt/BlockChain/Address/CSVData/bitcoin_address.csv")

# COMMAND ----------

address_bitcoin_cash_df[["address", "balance","Name"]].coalesce(1).write.option("header", "true").csv("dbfs:/mnt/BlockChain/Address/CSVData/bitcoin_cash_address.csv")

# COMMAND ----------

address_bitcoin_sv_df[["address", "balance","Name"]].coalesce(1).write.option("header", "true").csv("dbfs:/mnt/BlockChain/Address/CSVData/bitcoin_sv_address.csv")

# COMMAND ----------

address_dash_df[["address", "balance","Name"]].coalesce(1).write.option("header", "true").csv("dbfs:/mnt/BlockChain/Address/CSVData/dash_address.csv")

# COMMAND ----------

address_lite_coin_df[["address", "balance","Name"]].coalesce(1).write.option("header", "true").csv("dbfs:/mnt/BlockChain/Address/CSVData/litecoin_address.csv")

# COMMAND ----------

address_dogecoin_cash_df[["address", "balance","Name"]].coalesce(1).write.option("header", "true").csv("dbfs:/mnt/BlockChain/Address/CSVData/dogecoin_address_df.csv")

# COMMAND ----------

dbutils.fs.cp("dbfs:/mnt/BlockChain/Address/CSVData/bitcoin_address.csv", "dbfs:/FileStore/bitcoin_address.csv", recurse = True)
dbutils.fs.cp("dbfs:/mnt/BlockChain/Address/CSVData/bitcoin_cash_address.csv", "dbfs:/FileStore/bitcoin_cash_address.csv", recurse = True)
dbutils.fs.cp("dbfs:/mnt/BlockChain/Address/CSVData/bitcoin_sv_address.csv", "dbfs:/FileStore/bitcoin_sv_address.csv", recurse = True)
dbutils.fs.cp("dbfs:/mnt/BlockChain/Address/CSVData/dash_address.csv", "dbfs:/FileStore/dash_address.csv", recurse = True)
dbutils.fs.cp("dbfs:/mnt/BlockChain/Address/CSVData/litecoin_address.csv", "dbfs:/FileStore/litecoin_address.csv", recurse = True)
dbutils.fs.cp("dbfs:/mnt/BlockChain/Address/CSVData/dogecoin_address_df.csv", "dbfs:/FileStore/dogecoin_address_df.csv", recurse = True)

# COMMAND ----------

dbutils.fs.cp("dbfs:/mnt/BlockChain/Address/CSVData/dogecoin_address_df.csv", "dbfs:/FileStore/dogecoin_address_df.csv", recurse = True)
