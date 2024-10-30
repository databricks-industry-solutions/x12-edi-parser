# Databricks notebook source
# MAGIC %md # 837I and 837P

# COMMAND ----------

from databricksx12 import *
from databricksx12.hls import *
import json, os
from pyspark.sql.functions import input_file_name


hm = HealthcareManager()
df = spark.read.text("file:////Workspace/Users/aaron.zavora@databricks.com/x12-edi-parser/sampledata/837/*txt", wholetext = True)


rdd = (
 df.withColumn("filename", input_file_name()).rdd
  .map(lambda x: (x.asDict().get("filename"),x.asDict().get("value")))
  .map(lambda x: (x[0], EDI(x[1])))
  .map(lambda x: { **{'filename': x[0]}, **hm.to_json(x[1])} )
  .map(lambda x: json.dumps(x))
)
claims = spark.read.json(rdd)

# COMMAND ----------

claims.createOrReplaceTempView("edi")

# COMMAND ----------

# MAGIC %sql
# MAGIC select filename, edi_control_number, edi_date, edi_time, 
# MAGIC edi_recipient_qualifier_id, edi_sender_qualifier_id, edi_standard_version,
# MAGIC fgs.fg.`FunctionalGroup.control_number` as fg_control_number,
# MAGIC fgs.fg.`FunctionalGroup.date` as fg_date,
# MAGIC fgs.fg.`FunctionalGroup.time` as fg_time,
# MAGIC fgs.fg.`FunctionalGroup.receiver` as fg_receiver,
# MAGIC fgs.fg.`FunctionalGroup.sender` as fg_sender,
# MAGIC fgs.fg.`FunctionalGroup.standard_version` as fg_standard_version,
# MAGIC fgs.fg.`FunctionalGroup.transaction_type` as fg_transaction_type,
# MAGIC   explode(fgs.fg.`Transactions`) as trnx
# MAGIC from 
# MAGIC (
# MAGIC select
# MAGIC `edi.control_number` as edi_control_number,
# MAGIC `edi.date` as edi_date,
# MAGIC `edi.time` as edi_time,
# MAGIC `EDI.recipient_qualifier_id` as edi_recipient_qualifier_id,
# MAGIC `EDI.sender_qualifier_id` as edi_sender_qualifier_id,
# MAGIC `EDI.standard_version` as edi_standard_version,
# MAGIC explode(`FunctionalGroup`) as fg,
# MAGIC filename
# MAGIC from edi
# MAGIC ) fgs

# COMMAND ----------

# MAGIC %sql
# MAGIC --flatten EDI 
# MAGIC drop table if exists stg_claims;
# MAGIC CREATE TABLE stg_claims 
# MAGIC as 
# MAGIC select *
# MAGIC from 
# MAGIC (
# MAGIC select *, explode(trnx.Claims) as clms
# MAGIC from
# MAGIC (
# MAGIC select filename, edi_control_number, edi_date, edi_time, 
# MAGIC edi_recipient_qualifier_id, edi_sender_qualifier_id, edi_standard_version,
# MAGIC fgs.fg.`FunctionalGroup.control_number` as fg_control_number,
# MAGIC fgs.fg.`FunctionalGroup.date` as fg_date,
# MAGIC fgs.fg.`FunctionalGroup.time` as fg_time,
# MAGIC fgs.fg.`FunctionalGroup.receiver` as fg_receiver,
# MAGIC fgs.fg.`FunctionalGroup.sender` as fg_sender,
# MAGIC fgs.fg.`FunctionalGroup.standard_version` as fg_standard_version,
# MAGIC fgs.fg.`FunctionalGroup.transaction_type` as fg_transaction_type,
# MAGIC   explode(fgs.fg.`Transactions`) as trnx
# MAGIC from 
# MAGIC (
# MAGIC select
# MAGIC `edi.control_number` as edi_control_number,
# MAGIC `edi.date` as edi_date,
# MAGIC `edi.time` as edi_time,
# MAGIC `EDI.recipient_qualifier_id` as edi_recipient_qualifier_id,
# MAGIC `EDI.sender_qualifier_id` as edi_sender_qualifier_id,
# MAGIC `EDI.standard_version` as edi_standard_version,
# MAGIC explode(`FunctionalGroup`) as fg,
# MAGIC filename
# MAGIC from edi
# MAGIC ) fgs
# MAGIC ) trnx
# MAGIC ) clms 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from stg_claims limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC Claim Header Table

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists claim_header;
# MAGIC create table claim_header as 
# MAGIC select
# MAGIC filename, edi_control_number, edi_date, edi_time, edi_recipient_qualifier_id, edi_sender_qualifier_id, edi_standard_version,
# MAGIC fg_control_number, fg_date, fg_time, fg_receiver, fg_sender, fg_standard_version, fg_transaction_type,
# MAGIC clms.claim_header.*, 
# MAGIC clms.diagnosis.*,
# MAGIC clms.payer.*,
# MAGIC clms.providers.*,
# MAGIC   clms.patient.name as patient_name,  
# MAGIC   clms.patient.patient_relationship_cd,
# MAGIC   clms.patient.street as patient_street,
# MAGIC   clms.patient.city as patient_city,
# MAGIC   clms.patient.zip as patient_zip,
# MAGIC   clms.patient.dob as patient_dob,
# MAGIC   clms.patient.dob_format as patient_dob_format,
# MAGIC   clms.patient.gender_cd as patient_gender_cd,
# MAGIC   clms.subscriber.subsciber_identifier,
# MAGIC   clms.subscriber.name as subscriber_name,
# MAGIC   clms.subscriber.subscriber_relationship_cd,
# MAGIC   clms.subscriber.street as subscriber_street,
# MAGIC   clms.subscriber.city as subscriber_city,
# MAGIC   clms.subscriber.zip as subscriber_zip,
# MAGIC   clms.subscriber.dob as subscriber_dob,
# MAGIC   clms.subscriber.dob_format as subscriber_dob_format,
# MAGIC   clms.subscriber.gender_cd as subscriber_gender_cd
# MAGIC from stg_claims

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from claim_header limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC Claim Lines table

# COMMAND ----------

# MAGIC %sql
# MAGIC create table claim_line as 
# MAGIC select filename, claim_id, cl.*
# MAGIC from (
# MAGIC select filename, 
# MAGIC clms.claim_header.claim_id, 
# MAGIC explode(clms.claim_lines) as cl 
# MAGIC from stg_claims
# MAGIC ) foo

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from claim_line

# COMMAND ----------

# MAGIC %md # 835 

# COMMAND ----------

from databricksx12 import *
from databricksx12.hls import *
import json, os
from pyspark.sql.functions import input_file_name

hm = HealthcareManager()
df = spark.read.text("file:////Workspace/Users/aaron.zavora@databricks.com/x12-edi-parser/sampledata/835/*txt", wholetext = True)


rdd = (
 df.withColumn("filename", input_file_name()).rdd
  .map(lambda x: (x.asDict().get("filename"),x.asDict().get("value")))
  .map(lambda x: (x[0], EDI(x[1])))
  .map(lambda x: { **{'filename': x[0]}, **hm.to_json(x[1])} )
  .map(lambda x: json.dumps(x))
)
claims = spark.read.json(rdd)

# COMMAND ----------

claims.createOrReplaceTempView("edi")

# COMMAND ----------

# MAGIC %sql
# MAGIC --flatten EDI 
# MAGIC drop table if exists stg_remittance;
# MAGIC CREATE TABLE stg_remittance 
# MAGIC as 
# MAGIC select *
# MAGIC from 
# MAGIC (
# MAGIC select *, explode(trnx.Claims) as clms
# MAGIC from
# MAGIC (
# MAGIC select filename, edi_control_number, edi_date, edi_time, 
# MAGIC edi_recipient_qualifier_id, edi_sender_qualifier_id, edi_standard_version,
# MAGIC fgs.fg.`FunctionalGroup.control_number` as fg_control_number,
# MAGIC fgs.fg.`FunctionalGroup.date` as fg_date,
# MAGIC fgs.fg.`FunctionalGroup.time` as fg_time,
# MAGIC fgs.fg.`FunctionalGroup.receiver` as fg_receiver,
# MAGIC fgs.fg.`FunctionalGroup.sender` as fg_sender,
# MAGIC fgs.fg.`FunctionalGroup.standard_version` as fg_standard_version,
# MAGIC fgs.fg.`FunctionalGroup.transaction_type` as fg_transaction_type,
# MAGIC   explode(fgs.fg.`Transactions`) as trnx
# MAGIC from 
# MAGIC (
# MAGIC select
# MAGIC `edi.control_number` as edi_control_number,
# MAGIC `edi.date` as edi_date,
# MAGIC `edi.time` as edi_time,
# MAGIC `EDI.recipient_qualifier_id` as edi_recipient_qualifier_id,
# MAGIC `EDI.sender_qualifier_id` as edi_sender_qualifier_id,
# MAGIC `EDI.standard_version` as edi_standard_version,
# MAGIC explode(`FunctionalGroup`) as fg,
# MAGIC filename
# MAGIC from edi
# MAGIC ) fgs
# MAGIC ) trnx
# MAGIC ) clms 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from stg_remittance limit 10
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists remittance;
# MAGIC create table remittance as 
# MAGIC select filename, edi_control_number, edi_date, edi_time, edi_recipient_qualifier_id, edi_sender_qualifier_id, edi_standard_version,
# MAGIC fg_control_number, fg_date, fg_time, fg_receiver, fg_sender, fg_standard_version, fg_transaction_type,
# MAGIC clms.*
# MAGIC from stg_remittance

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from remittance
