![image](https://raw.githubusercontent.com/databricks-industry-solutions/.github/main/profile/solacc_logo_wide.png)

[![CLOUD](https://img.shields.io/badge/CLOUD-ALL-blue?logo=googlecloud&style=for-the-badge)](https://cloud.google.com/databricks)
[![POC](https://img.shields.io/badge/POC-10_days-green?style=for-the-badge)](https://databricks.com/try-databricks)

# Business Problem 

Working with various x12 EDI transactions in Spark on Databricks.

# Install

```python
pip install git+https://github.com/databricks-industry-solutions/x12-edi-parser
```

# Run 

## Reading in EDI Data

Default format used is AnsiX12 (* as a delim and ~ as segment separator)

```python
from databricksx12 import *

#EDI format type
ediFormat = AnsiX12Delim #specifying formats of data, ansi is also the default if nothing is specified
#can also specify customer formats (below is the same as AnsiX12Delim)
ediFormat = type("", (), dict({'SEGMENT_DELIM': '~', 'ELEMENT_DELIM': '*', 'SUB_DELIM': ':'}))

df = spark.read.text("sampledata/837/*txt", wholetext = True)

(df.rdd
  .map(lambda x: x.asDict().get("value"))
  .map(lambda x: EDI(x, delim_cls = ediFormat))
  .map(lambda x: {"transaction_count": x.num_transactions()})
).toDF().show()
"""
+-----------------+
|transaction_count|
+-----------------+
|                5|
|                1|
|                1|
|                1|
|                1|
+-----------------+
"""
```

## Parsing Healthcare Transactions

Currently supports 837s. Records in each format type should be saved separately, e.g. do not mix 835s & 837s in the df.save() command.

### 837i and 837p sample data in Spark

```python
from databricksx12 import *
from databricksx12.hls import *
import json
from pyspark.sql.functions import input_file_name

hm = HealthcareManager()
df = spark.read.text("sampledata/837/*txt", wholetext = True)


rdd = (
 df.withColumn("filename", input_file_name()).rdd
  .map(lambda x: (x.asDict().get("filename"),x.asDict().get("value")))
  .map(lambda x: (x[0], EDI(x[1])))
  .map(lambda x: { **{'filename': x[0]}, **hm.to_json(x[1])} )
  .map(lambda x: json.dumps(x))
)
claims = spark.read.json(rdd)

#Claim header table TODO 

#Claim line table TODO 

```

### Sample data outside of Spark


```python
from databricksx12 import *
from databricksx12.hls import *
import json

hm = HealthcareManager()
edi =  EDI(open("sampledata/837/CHPW_Claimdata.txt", "rb").read().decode("utf-8"))

#Returns parsed claim data
hm.from_edi(edi) 
#[<databricksx12.hls.claim.Claim837p object at 0x106e57b50>, <databricksx12.hls.claim.Claim837p object at 0x106e57c40>, <databricksx12.hls.claim.Claim837p object at 0x106e57eb0>, <databricksx12.hls.claim.Claim837p object at 0x106e57b20>, <databricksx12.hls.claim.Claim837p object at 0x106e721f0>]

#Print in json format
print(json.dumps(hm.to_json(edi), indent=4)) 

"""
{
    "EDI.sender_tax_id": "ZZ",
    "FuncitonalGroup": [
        {
            "FunctionalGroup.receiver": "123456789",
            "FunctionalGroup.sender": "CLEARINGHOUSE",
            "FunctionalGroup.transaction_datetime": "20180508:0833",
            "FunctionalGroup.transaction_type": "222",
            "Transactions": [
                {
                    "Transaction.transaction_type": "222",
                    "Claims": [
                        {
                            "submitter": {
                                "contact_name": "CLEARINGHOUSE CLIENT SERVICES",
                                "contacts": {
                                    "primary": [
                                        {
                                            "contact_method": "Telephone",
                                            "contact_number": "8005551212",
...
"""

#print the raw EDI Segments of one claim
one_claim = hm.from_edi(edi)[0]
print("\n".join([y.data for y in one_claim.data])) #Print one claim to look at the segments of it
"""
BHT*0019*00*7349063984*20180508*0833*CH
NM1*41*2*CLEARINGHOUSE LLC*****46*987654321
PER*IC*CLEARINGHOUSE CLIENT SERVICES*TE*8005551212*FX*8005551212
NM1*40*2*123456789*****46*CHPWA
HL*1**20*1
NM1*85*2*BH CLINIC OF VANCOUVER*****XX*1122334455
N3*12345 MAIN ST
N4*VANCOUVER*WA*98662
REF*EI*720000000
PER*IC*CONTACT*TE*9185551212
NM1*87*2
N3*PO BOX 1234
N4*VANCOUVER*WA*986681234
HL*2*1*22*0
SBR*P*18**COMMUNITY HLTH PLAN OF WASH*****CI
NM1*IL*1*SUBSCRIBER*JOHN*J***MI*987321
N3*987 65TH PL
...
"""
```

## EDI as a Table for SQL

```python
""""
Look at all data refernce -> https://justransform.com/edi-essentials/edi-structure/
  (1) Including control header / ISA & IEA segments
"""
from pyspark.sql.functions import input_file_name

( df.withColumn("filename", input_file_name()).rdd
  .map(lambda x: (x.asDict().get("filename"),x.asDict().get("value")))
  .map(lambda x: (x[0], EDI(x[1])))
  .map(lambda x: [{**{"filename": x[0]}, **y} for y in x[1].toRows()])
  .flatMap(lambda x: x)
  .toDF()).show()

"""
+--------------------+----------+--------------------------+--------------+------------+-----------------------------+--------+
|            row_data|row_number|segment_element_delim_char|segment_length|segment_name|segment_subelement_delim_char|filename|
+--------------------+----------+--------------------------+--------------+------------+-----------------------------+--------+
|ISA*00*          ...|         0|                         *|            17|         ISA|                            :|file:///|
|GS*HC*CLEARINGHOU...|         1|                         *|             9|          GS|                            :|file:///|
|ST*837*000000001*...|         2|                         *|             4|          ST|                            :|file:///|
|BHT*0019*00*73490...|         3|                         *|             7|         BHT|                            :|file:///|
|NM1*41*2*CLEARING...|         4|                         *|            10|         NM1|                            :|file:///|
|PER*IC*CLEARINGHO...|         5|                         *|             7|         PER|                            :|file:///|
|NM1*40*2*12345678...|         6|                         *|            10|         NM1|                            :|file:///|
```

#### Other EDI Parsing in Pyspark

```python

"""
# (2) Individual Transactions (Functional header) / ST & SE segments
"""
trxDF = ( df.withColumn("filename", input_file_name()).rdd
  .map(lambda x: (x.asDict().get("filename"),x.asDict().get("value")))
  .map(lambda x: (x[0], EDI(x[1])))
  .map(lambda x: [(x[0], y) for y in x[1].transaction_segments()])
  .flatMap(lambda x: x)
  .map(lambda x: [{**{"filename": x[0]}, **y} for y in x[1].toRows()])
  .flatMap(lambda x: x)
  .toDF())

trxDF.show()
"""
Includes filename column but not shown below
+--------------------+----------+--------------------------+--------------+------------+-----------------------------+
|            row_data|row_number|segment_element_delim_char|segment_length|segment_name|segment_subelement_delim_char|
+--------------------+----------+--------------------------+--------------+------------+-----------------------------+
|ST*837*000000001*...|         0|                         *|             4|          ST|                            :|
|BHT*0019*00*73490...|         1|                         *|             7|         BHT|                            :|
|NM1*41*2*CLEARING...|         2|                         *|            10|         NM1|                            :|
|PER*IC*CLEARINGHO...|         3|                         *|             7|         PER|                            :|
|NM1*40*2*12345678...|         4|                         *|            10|         NM1|                            :|
|          HL*1**20*1|         5|                         *|             5|          HL|                            :|
"""

#read EDI and save predefined fields to DF (WIP) 
df = spark.read.text("sampledata/837/*", wholetext = True)
ediDF = (
  df.rdd
    .map(lambda x: x.asDict().get("value"))
    .map(lambda x: EDI(x))
    .map(lambda x: x.toJson())
).toDF()


ediDF.show()
"""
+--------------------+--------------------+
|edi_transaction_type|transaction_datetime|
+--------------------+--------------------+
|                837P|       20180508:0833|
|                837P|     20180710:214339|
|                837I|   20180807:12022761|
|                837P|   20180807:12022605|
+--------------------+--------------------+
"""

#Count number of transactions
(df.rdd
  .map(lambda x: x.asDict().get("value"))
  .map(lambda x: EDI(x))
  .map(lambda x: {"transaction_count": x.num_transactions()})
).toDF().show()
"""
+-----------------+
|transaction_count|
+-----------------+
|                5|
|                1|
|                1|
|                1|
+-----------------+
"""


#show first line of each transaction
trxDF.filter(x.row_number == 0).show()
"""
+--------------------+----------+--------------------------+--------------+------------+-----------------------------+
|            row_data|row_number|segment_element_delim_char|segment_length|segment_name|segment_subelement_delim_char|
+--------------------+----------+--------------------------+--------------+------------+-----------------------------+
|ST*837*000000001*...|         0|                         *|             4|          ST|                            :|
|ST*837*000000002*...|         0|                         *|             4|          ST|                            :|
|ST*837*000000003*...|         0|                         *|             4|          ST|                            :|
|ST*837*000000004*...|         0|                         *|             4|          ST|                            :|
|ST*837*000000005*...|         0|                         *|             4|          ST|                            :|
|ST*837*000000001*...|         0|                         *|             4|          ST|                            :|
|ST*837*0001*00501...|         0|                         *|             4|          ST|                            :|
|ST*837*0001*00501...|         0|                         *|             4|          ST|                            :|
|ST*837*0001*00501...|         0|                         *|             4|          ST|                            :|
+--------------------+----------+--------------------------+--------------+------------+-----------------------------+
"""
``` 

### 

## Project support 

Please note the code in this project is provided for your exploration only, and are not formally supported by Databricks with Service Level Agreements (SLAs). They are provided AS-IS and we do not make any guarantees of any kind. Please do not submit a support ticket relating to any issues arising from the use of these projects. The source in this project is provided subject to the Databricks [License](./LICENSE). All included or referenced third party libraries are subject to the licenses set forth below.

Any issues discovered through the use of this project should be filed as GitHub Issues on the Repo. They will be reviewed as time permits, but there are no formal SLAs for support. 
