# Databricks notebook source
# MAGIC %md # 837i and 837p

# COMMAND ----------

# DBTITLE 1,Install package
# MAGIC %pip install git+https://github.com/databricks-industry-solutions/x12-edi-parser.git

# COMMAND ----------

# DBTITLE 1,Read in sample 837 data
from databricksx12 import *
from databricksx12.hls import *
import json, os
from pyspark.sql.functions import input_file_name

#hm manages the parsing of different formats
hm = HealthcareManager()
df = spark.read.text("file:///" + os.getcwd() + "/sampledata/837/*txt", wholetext = True)

rdd = (
  df.withColumn("filename", input_file_name()).rdd
  .map(lambda row: (row.filename, EDI(row.value)))
  .map(lambda edi: hm.flatten(edi[1], filename = edi[0]))
  .flatMap(lambda x: x)
)

claims_rdd = (
rdd.repartition(4)
  .map(lambda x: hm.flatten_to_json(x))
  .map(lambda x: json.dumps(x))
)
claims = spark.read.json(claims_rdd)

# COMMAND ----------

# DBTITLE 1,Save as a view
claims.createOrReplaceTempView("stg_claims")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from stg_claims

# COMMAND ----------

# DBTITLE 1,Create Claim Header
# MAGIC %sql
# MAGIC drop table if exists claim_header;
# MAGIC create table claim_header as 
# MAGIC select * except(claim_lines)
# MAGIC from stg_claims
# MAGIC ;
# MAGIC
# MAGIC SELECT * FROM claim_header

# COMMAND ----------

# DBTITLE 1,Create Claim Line
# MAGIC %sql
# MAGIC drop table if exists claim_line;
# MAGIC create table claim_line as 
# MAGIC select *  except(claim_header, claim_lines)
# MAGIC from (
# MAGIC select *, explode(claim_lines) as claim_line
# MAGIC from stg_claims
# MAGIC )
# MAGIC ;
# MAGIC
# MAGIC SELECT * FROM claim_line

# COMMAND ----------

# MAGIC %md # 835 

# COMMAND ----------

# DBTITLE 1,Read in sample 835 data
from databricksx12 import *
from databricksx12.hls import *
import json, os
from pyspark.sql.functions import input_file_name

hm = HealthcareManager()
df = spark.read.text(df = spark.read.text("file:///" + os.getcwd() + "/sampledata/835/*txt", wholetext = True)


rdd = (
df.withColumn("filename", input_file_name()).rdd
  .map(lambda row: (row.filename, EDI(row.value, strict_transactions=False))) #strict_transactions = False ignores if SE01 is an incorrect value (shoudld be set to number of segments in transaction)
  .map(lambda edi: hm.flatten(edi[1], filename = edi[0]))
  .flatMap(lambda x: x)
)

claims_rdd = (
  rdd.repartition(4)
  .map(lambda x: hm.flatten_to_json(x))
  .map(lambda x: json.dumps(x))
)

claims = spark.read.json(claims_rdd)

# COMMAND ----------

# DBTITLE 1,Save as a view
claims.createOrReplaceTempView("stg_remittance")

# COMMAND ----------

# DBTITLE 1,Create Remittance
# MAGIC %sql
# MAGIC drop table if exists remittance;
# MAGIC CREATE TABLE remittance 
# MAGIC as 
# MAGIC select *
# MAGIC from stg_remittance 
# MAGIC ;
# MAGIC
# MAGIC SELECT * FROM remittance;
