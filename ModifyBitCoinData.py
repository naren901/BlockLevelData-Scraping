# Databricks notebook source
from pyspark.sql import SparkSession
import pyspark.sql.types as st
from pyspark.sql.types import StructType,StructField, StringType
from pyspark.sql import  functions
from pyspark.sql.functions import col, lower, to_date, to_timestamp, to_utc_timestamp, month, year, weekofyear, dayofmonth 

# COMMAND ----------

# MAGIC %md
# MAGIC ## EXTEND CURRENT DF

# COMMAND ----------

# MAGIC %md
# MAGIC ####1) Load block level data for all coins from MNT location
# MAGIC ####2) Get unique miners list for all the coins

# COMMAND ----------

#bitcoin
bitcoin_block_df = spark.read.csv(path="dbfs:/mnt/BlockChain/Blocks/BitCoin/bitcoin_block_tsv_rdd_df.csv", header=True,  inferSchema= True, sep=",")

# COMMAND ----------

#bitcoin cash
bitcoin_cash_block_df = spark.read.csv(path="dbfs:/mnt/BlockChain/Blocks/Bitcoin-cash/bc_block_sub_df_exported.csv", header=True,  inferSchema= True, sep=",")

# COMMAND ----------

#bitcoin SV
bitcoin_sv_block_df = spark.read.csv(path="dbfs:/mnt/BlockChain/Blocks/BC-SV/bcsv_block_tsv_rdd_df.csv", header=True,  inferSchema= True, sep=",")

# COMMAND ----------

#dash
dash_block_df = spark.read.csv(path="dbfs:/mnt/BlockChain/Blocks/Dash/dash_block_tsv_rdd_df.csv", header=True,  inferSchema= True, sep=",")

# COMMAND ----------

#dogecoin
dogecoin_block_df = spark.read.csv(path="dbfs:/mnt/BlockChain/Blocks/Doge-Coin/dogecoin_block_tsv_rdd_df.csv", header=True,  inferSchema= True, sep=",")

# COMMAND ----------

#ethereum
ehtereum_block_df = spark.read.csv(path="dbfs:/mnt/BlockChain/Blocks/Ethereum/df_ethereum_block_tsv_rdd_df.csv", header=True,  inferSchema= True, sep=",")

# COMMAND ----------

#lite coin
litecoin_block_df = spark.read.csv(path="dbfs:/mnt/BlockChain/Blocks/LiteCoin/block_df.csv", header=True,  inferSchema= True, sep=",")


# COMMAND ----------

bitcoin_miner_grp = sorted(bitcoin_block_df.groupBy("guessed_miner").count().collect(), key = lambda row: row["count"], reverse= True)
display(bitcoin_miner_grp)

# COMMAND ----------

bitcoin_cash_miner_grp = sorted(bitcoin_cash_block_df.groupBy("guessed_miner").count().collect(), key= lambda row: row["count"], reverse= True)
display(bitcoin_cash_miner_grp)

# COMMAND ----------

bitcoin_SV_miners_grp = sorted(bitcoin_sv_block_df.groupBy("guessed_miner").count().collect(), key= lambda row: row["count"], reverse= True)
display(bitcoin_SV_miners_grp)

# COMMAND ----------

dash_miners_grp = sorted(dash_block_df.groupBy("guessed_miner").count().collect(), key= lambda row: row["count"], reverse= True)
display(dash_miners_grp)

# COMMAND ----------

dogecoin_miners_grp = sorted(dogecoin_block_df.groupBy("guessed_miner").count().collect(), key= lambda row: row["count"], reverse= True)
display(dogecoin_miners_grp)

# COMMAND ----------

ethereum_miners_grp = sorted(ehtereum_block_df.groupBy("miner").count().collect(), key= lambda row: row["count"], reverse= True)
display(ethereum_miners_grp)

# COMMAND ----------

litecoin_miners_grp = sorted(litecoin_block_df.groupBy("guessed_miner").count().collect(), key= lambda row: row["count"], reverse= True)
display(litecoin_miners_grp)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ADD Year, Month, Week and isPool columns to existing DF

# COMMAND ----------

# MAGIC %md
# MAGIC #### BITCION DF EXTESNION

# COMMAND ----------

bitcoin_block_extended_df = bitcoin_block_df.withColumn("Date", to_date(col("time"), format= "yyyy-MM-dd HH:mm:ss"))\
                                       .withColumn("Year",  year(col("Date")))\
                                       .withColumn("Month",month(col("Date")))\
                                       .withColumn("Week_of_Year", weekofyear(col("Date")))\
                                       .withColumn("isPool", lower(col("guessed_miner")).contains("pool"))

# COMMAND ----------

display(bitcoin_block_extended_df.show(10))

# COMMAND ----------

bitcoin_block_extended_df.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("dbfs:/mnt/BlockChain/Blocks/BitCoin/" + "bitcoin_block_extended_df.csv")

# COMMAND ----------

dbutils.fs.ls("dbfs:/mnt/BlockChain/Blocks/BitCoin/bitcoin_block_extended_df.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #### BITCOIN CASH DF EXTENSION

# COMMAND ----------

bitcoin_cash_block_extended_df = bitcoin_cash_block_df.withColumn("Date", to_date(col("time"), format= "yyyy-MM-dd HH:mm:ss"))\
                                       .withColumn("Year",  year(col("Date")))\
                                       .withColumn("Month",month(col("Date")))\
                                       .withColumn("Week_of_Year", weekofyear(col("Date")))\
                                       .withColumn("isPool", lower(col("guessed_miner")).contains("pool"))

# COMMAND ----------

display(bitcoin_cash_block_extended_df.show(10))

# COMMAND ----------

bitcoin_cash_block_extended_df.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("dbfs:/mnt/BlockChain/Blocks/Bitcoin-cash/bitcoin_cash_block_extended_df.csv")


# COMMAND ----------

dbutils.fs.ls("dbfs:/mnt/BlockChain/Blocks/Bitcoin-cash/bitcoin_cash_block_extended_df.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #### BITCOIN-SV DF EXTENSION

# COMMAND ----------

bitcoin_sv_block_extended_df = bitcoin_sv_block_df.withColumn("Date", to_date(col("time"), format= "yyyy-MM-dd HH:mm:ss"))\
                                       .withColumn("Year",  year(col("Date")))\
                                       .withColumn("Month",month(col("Date")))\
                                       .withColumn("Week_of_Year", weekofyear(col("Date")))\
                                       .withColumn("isPool", lower(col("guessed_miner")).contains("pool"))

# COMMAND ----------

display(bitcoin_sv_block_extended_df.show(10))

# COMMAND ----------

bitcoin_sv_block_extended_df.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("dbfs:/mnt/BlockChain/Blocks/BC-SV/bitcoin_sv_block_extended_df.csv")
dbutils.fs.ls("dbfs:/mnt/BlockChain/Blocks/BC-SV/bitcoin_sv_block_extended_df.csv")


# COMMAND ----------

# MAGIC %md
# MAGIC #### DASH DF EXTENSION

# COMMAND ----------

dash_block_extended_df = dash_block_df.withColumn("Date", to_date(col("time"), format= "yyyy-MM-dd HH:mm:ss"))\
                                       .withColumn("Year",  year(col("Date")))\
                                       .withColumn("Month",month(col("Date")))\
                                       .withColumn("Week_of_Year", weekofyear(col("Date")))\
                                       .withColumn("isPool", lower(col("guessed_miner")).contains("pool"))

# COMMAND ----------

display(dash_block_extended_df.show(10))

# COMMAND ----------

dash_block_extended_df.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("dbfs:/mnt/BlockChain/Blocks/Dash/dash_block_extended_df.csv")
dbutils.fs.ls("dbfs:/mnt/BlockChain/Blocks/Dash/dash_block_extended_df.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #### DOGE COIN DF EXTENSION

# COMMAND ----------

dogecoin_block_extended_df = dogecoin_block_df.withColumn("Date", to_date(col("time"), format= "yyyy-MM-dd HH:mm:ss"))\
                                       .withColumn("Year",  year(col("Date")))\
                                       .withColumn("Month",month(col("Date")))\
                                       .withColumn("Week_of_Year", weekofyear(col("Date")))\
                                       .withColumn("isPool", lower(col("guessed_miner")).contains("pool"))

# COMMAND ----------

display(dogecoin_block_extended_df.show(10))

# COMMAND ----------

dogecoin_block_extended_df.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("dbfs:/mnt/BlockChain/Blocks/Doge-Coin/dogecoin_block_extended_df.csv")
dbutils.fs.ls("dbfs:/mnt/BlockChain/Blocks/Doge-Coin/dogecoin_block_extended_df.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ####ETHEREUM DF EXTENSION

# COMMAND ----------

ehtereum_block_extended_df =  ehtereum_block_df.withColumn("Date", to_date(col("time"), format= "yyyy-MM-dd HH:mm:ss"))\
                                       .withColumn("Year",  year(col("Date")))\
                                       .withColumn("Month",month(col("Date")))\
                                       .withColumn("Week_of_Year", weekofyear(col("Date")))\
                                       .withColumn("isPool", lower(col("miner")).contains("pool"))

# COMMAND ----------

display(ehtereum_block_extended_df.show(10))

# COMMAND ----------

ehtereum_block_extended_df.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("dbfs:/mnt/BlockChain/Blocks/Ethereum/ehtereum_block_extended_df.csv")
dbutils.fs.ls("dbfs:/mnt/BlockChain/Blocks/Ethereum/ehtereum_block_extended_df.csv")

# COMMAND ----------

dbutils.fs.ls("dbfs:/mnt/BlockChain/Blocks/Ethereum/ehtereum_block_extended_df.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #### LITECOIN DF EXTENSION

# COMMAND ----------

litecoin_block_extended_df = litecoin_block_df.withColumn("Date", to_date(col("time"), format= "yyyy-MM-dd HH:mm:ss"))\
                                       .withColumn("Year",  year(col("Date")))\
                                       .withColumn("Month",month(col("Date")))\
                                       .withColumn("Week_of_Year", weekofyear(col("Date")))\
                                       .withColumn("isPool", lower(col("guessed_miner")).contains("pool"))

# COMMAND ----------

display(litecoin_block_extended_df.show(10))

# COMMAND ----------

litecoin_block_extended_df.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("dbfs:/mnt/BlockChain/Blocks/LiteCoin/litecoin_block_extended_df.csv")
dbutils.fs.ls("dbfs:/mnt/BlockChain/Blocks/LiteCoin/litecoin_block_extended_df.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #### COPY TO DOWNLOAD FOLDER

# COMMAND ----------

dbutils.fs.cp("dbfs:/mnt/BlockChain/Blocks/BC-SV/bitcoin_sv_block_extended_df.csv", "dbfs:/FileStore/bitcoin_sv_block_extended_df.csv", recurse = True )

# COMMAND ----------

dbutils.fs.cp("dbfs:/mnt/BlockChain/Blocks/BitCoin/bitcoin_block_extended_df.csv",  "dbfs:/FileStore/bitcoin_block_extended_df.csv", recurse = True )

# COMMAND ----------

dbutils.fs.cp("dbfs:/mnt/BlockChain/Blocks/Bitcoin-cash/bitcoin_cash_block_extended_df.csv",  "dbfs:/FileStore/bitcoin_cash_block_extended_df.csv", recurse = True )

# COMMAND ----------

dbutils.fs.cp("dbfs:/mnt/BlockChain/Blocks/Dash/dash_block_extended_df.csv",  "dbfs:/FileStore/dash_block_extended_df.csv", recurse = True )

# COMMAND ----------

dbutils.fs.cp("dbfs:/mnt/BlockChain/Blocks/Doge-Coin/dogecoin_block_extended_df.csv",  "dbfs:/FileStore/dogecoin_block_extended_df.csv", recurse = True )

# COMMAND ----------

dbutils.fs.cp("dbfs:/mnt/BlockChain/Blocks/Ethereum/ehtereum_block_extended_df.csv",  "dbfs:/FileStore/ehtereum_block_extended_df.csv", recurse = True )

# COMMAND ----------

dbutils.fs.cp("dbfs:/mnt/BlockChain/Blocks/LiteCoin/litecoin_block_extended_df.csv",  "dbfs:/FileStore/litecoin_block_extended_df.csv", recurse = True )

# COMMAND ----------

dash_extended_preview_df = spark.read.csv("dbfs:/mnt/BlockChain/Blocks/BitCoin/bitcoin_block_extended_df.csv", inferSchema= True, header= True)

# COMMAND ----------

display(dash_extended_preview_df.select("month").distinct().collect())

# COMMAND ----------

dash_extended_preview_df.columns

# COMMAND ----------

display(dash_extended_preview_df.select("Date", "Week_of_Year").distinct().collect())

# COMMAND ----------


