![image](https://raw.githubusercontent.com/databricks-industry-solutions/.github/main/profile/solacc_logo_wide.png)

[![CLOUD](https://img.shields.io/badge/CLOUD-ALL-blue?logo=googlecloud&style=for-the-badge)](https://cloud.google.com/databricks)
[![POC](https://img.shields.io/badge/POC-10_days-green?style=for-the-badge)](https://databricks.com/try-databricks)

## Business Problem

Addressing the issue of working with various parts of an x12 EDI transaction in Spark on Databricks.

## Install

## Run (Under Construction / Not Stable)

### Reading in EDI Data

```python
from databricksx12.edi import *

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

""""
Look at all data refernce -> https://justransform.com/edi-essentials/edi-structure/
  (1) Including control header / ISA & IEA segments
"""
( df.rdd
  .map(lambda x: x.asDict().get("value"))
  .map(lambda x: EDI(x))
  .map(lambda x: x.toRows())
  .flatMap(lambda x: x)
  .toDF()).show()

# (2) Functional header / ST & SE segments
trxDF = ( df.rdd
  .map(lambda x: x.asDict().get("value"))
  .map(lambda x: EDI(x))
  .map(lambda x: x.transaction_segments())
  .flatMap(lambda x: x)
  .map(lambda x: x.toRows())
  .flatMap(lambda x: x)
  .toDF())

trxDF.show()
"""
+--------------------+----------+--------------------------+--------------+------------+-----------------------------+
|            row_data|row_number|segment_element_delim_char|segment_length|segment_name|segment_subelement_delim_char|
+--------------------+----------+--------------------------+--------------+------------+-----------------------------+
|ST*837*000000001*...|         0|                         *|             4|          ST|                            :|
|BHT*0019*00*73490...|         1|                         *|             7|         BHT|                            :|
|NM1*41*2*CLEARING...|         2|                         *|            10|         NM1|                            :|
|PER*IC*CLEARINGHO...|         3|                         *|             7|         PER|                            :|
|NM1*40*2*12345678...|         4|                         *|            10|         NM1|                            :|
|          HL*1**20*1|         5|                         *|             5|          HL|                            :|
|NM1*85*2*BH CLINI...|         6|                         *|            10|         NM1|                            :|
|    N3*12345 MAIN ST|         7|                         *|             2|          N3|                            :|
|N4*VANCOUVER*WA*9...|         8|                         *|             4|          N4|                            :|
|    REF*EI*720000000|         9|                         *|             3|         REF|                            :|
|PER*IC*CONTACT*TE...|        10|                         *|             5|         PER|                            :|
|            NM1*87*2|        11|                         *|             3|         NM1|                            :|
|      N3*PO BOX 1234|        12|                         *|             2|          N3|                            :|
|N4*VANCOUVER*WA*9...|        13|                         *|             4|          N4|                            :|
|         HL*2*1*22*0|        14|                         *|             5|          HL|                            :|
|SBR*P*18**COMMUNI...|        15|                         *|            10|         SBR|                            :|
|NM1*IL*1*SUBSCRIB...|        16|                         *|            10|         NM1|                            :|
|      N3*987 65TH PL|        17|                         *|             2|          N3|                            :|
|N4*VANCOUVER*WA*9...|        18|                         *|             4|          N4|                            :|
|   DMG*D8*19881225*M|        19|                         *|             4|         DMG|                            :|
+--------------------+----------+--------------------------+--------------+------------+-----------------------------+
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

### Different EDI Formats

Default used is AnsiX12 (* as a delim and ~ as segment separator)

```python
from databricksx12.format import *
ediFormat = AnsiX12Delim #specifying formats of data  

(df.rdd
  .map(lambda x: x.asDict().get("value"))
  .map(lambda x: EDI(x), delim_cls = ediFormat)
  .map(lambda x: {"transaction_count": x.num_transactions()})
).toDF().show()
```

### 

## Project support 

Please note the code in this project is provided for your exploration only, and are not formally supported by Databricks with Service Level Agreements (SLAs). They are provided AS-IS and we do not make any guarantees of any kind. Please do not submit a support ticket relating to any issues arising from the use of these projects. The source in this project is provided subject to the Databricks [License](./LICENSE). All included or referenced third party libraries are subject to the licenses set forth below.

Any issues discovered through the use of this project should be filed as GitHub Issues on the Repo. They will be reviewed as time permits, but there are no formal SLAs for support. 
