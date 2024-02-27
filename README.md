![image](https://raw.githubusercontent.com/databricks-industry-solutions/.github/main/profile/solacc_logo_wide.png)

[![CLOUD](https://img.shields.io/badge/CLOUD-ALL-blue?logo=googlecloud&style=for-the-badge)](https://cloud.google.com/databricks)
[![POC](https://img.shields.io/badge/POC-10_days-green?style=for-the-badge)](https://databricks.com/try-databricks)

## Business Problem

Addressing the issue of working with various parts of an x12 EDI transaction in Spark on Databricks.

## Install

## Run (Under Construction / Not Stable)

### Reading in EDI Data

```python
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
