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

### 837i and 837p sample data in Spark

> [!NOTE]
> The recommended parsing path has been updated to be more effecient. You can still reference the previous approach in [this](https://github.com/databricks-industry-solutions/x12-edi-parser/commit/544f48e3cb9ebcf01027adb0867a3a2d6c0e768c) commit.


```python
from databricksx12 import *
from databricksx12.hls import *
import json
from pyspark.sql.functions import input_file_name

#This class manages how the different formats are parsed together
hm = HealthcareManager()
df = spark.read.text("sampledata/837/*txt", wholetext = True)

rdd = (
df.withColumn("filename", input_file_name()).rdd
.map(lambda row: (row.filename, EDI(row.value)))
.map(lambda edi: hm.flatten(edi[1], filename = edi[0]))
.flatMap(lambda x: x)
)

```

The **rdd** variable above will parse one file togther. To increase parallelism and reduce data skew, it is highly recommended to repartition like below for optimal performance

```python

claims_rdd = (
rdd.repartition() #Repartition number should be >= # of cores in cluster and <= number of rows in rdd / DataFrame
.map(lambda x: hm.flatten_to_json(x))
.map(lambda x: json.dumps(x))
)

claims = spark.read.json(claims_rdd)
```

> [!NOTE]
> Some EDI data does not have the correct SE01 value (number of segments in transaction). This check can be disabled by passing in strict_transactions=False e.g. EDI(...,strict_transactions=False)


### Creating claim header and line tables from the parser 

```python
claims.createOrReplaceTempView("stg_claims")
```

``` SQL
%sql
drop table if exists claim_header;
create table claim_header as 
select * except(claim_lines)
from stg_claims
;

SELECT * FROM claim_header;


drop table if exists claim_line;
create table claim_line as 
select *  except(claim_header)
from (
select *, explode(claim_lines) as claim_line
from stg_claims
)
;

SELECT * FROM claim_line

```

### Sample Output and Data Dictionary

![image](images/claim_header2.png?raw=true)
![image](images/claim_line2.png?raw=true)

### 835 sample

The steps for 835 are the same as 837s

```python
claims.createOrReplaceTempView("stg_claims")
```
``` SQL
%sql
drop table if exists remittance;
CREATE TABLE remittance 
as 
select *
from stg_remittance 
;

SELECT * FROM remittance;
```

> [!CAUTION]
> When reading in a small dataset and the repeating segments, like PLB (provider level adjustments) are not populated, the arrays for the column 'provider_adjustments' are defaulted to arrays of String value. To correct this behavior, you can explicitly specify the schema so that the 'provider_adjustments' column matches expected schema.

e.g.
```python
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

provider_adjustments_schema = ArrayType(
  StructType([StructField("adjustment_amount", StringType()),
              StructField("adjustment_grp_cd", StringType()),
              StructField("adjustment_reason_cd", StringType())]))

s = spark.read.json(claims_rdd).schema
s['provider_adjustments'].dataType = provider_adjustments_schema
claims = spark.read.schema(s).json(claims_rdd)

claims.select("provider_adjustments").printSchema()
#root
# |-- provider_adjustments: array (nullable = true)
# |    |-- element: struct (containsNull = true)
# |    |    |-- adjustment_amount: string (nullable = true)
# |    |    |-- adjustment_grp_cd: string (nullable = true)
# |    |    |-- adjustment_reason_cd: string (nullable = true)
```

![image](images/remittance_2.png?raw=true)


## Reading & Parsing Healthcare Transactions

Currently supports 837s and 835s. Records in each format type are recommended to be saved separately to avoid any ambiguity and opaqueness, e.g. do not to mix 835, 837i, 837p in df.save() command.  

## Sample data outside of Spark

```python
from databricksx12 import *
from databricksx12.hls import *
import json

hm = HealthcareManager()
edi =  EDI(open("sampledata/837/CHPW_Claimdata.txt", "rb").read().decode("utf-8"))

#Returns parsed claim data
hm.from_edi(edi) 
#[<databricksx12.hls.claim.Claim837p object at 0x106e57b50>, <databricksx12.hls.claim.Claim837p object at 0x106e57c40>, <databricksx12.hls.claim.Claim837p object at 0x106e57eb0>, <databricksx12.hls.claim.Claim837p object at 0x106e57b20>, <databricksx12.hls.claim.Claim837p object at 0x106e721f0>]


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

## Raw EDI as a Table 

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

## Project support 

Please note the code in this project is provided for your exploration only, and are not formally supported by Databricks with Service Level Agreements (SLAs). They are provided AS-IS and we do not make any guarantees of any kind. Please do not submit a support ticket relating to any issues arising from the use of these projects. The source in this project is provided subject to the Databricks [License](./LICENSE). All included or referenced third party libraries are subject to the licenses set forth below.

Any issues discovered through the use of this project should be filed as GitHub Issues on the Repo. They will be reviewed as time permits, but there are no formal SLAs for support. 
