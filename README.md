![image](https://raw.githubusercontent.com/databricks-industry-solutions/.github/main/profile/solacc_logo_wide.png)

[![CLOUD](https://img.shields.io/badge/CLOUD-ALL-blue?logo=googlecloud&style=for-the-badge)](https://cloud.google.com/databricks)
[![POC](https://img.shields.io/badge/POC-10_days-green?style=for-the-badge)](https://databricks.com/try-databricks)

# Databricks Ember for EDI Formats

Working with various x12 EDI transactions in Spark on Databrick with databricksx12 / ember python package. Supporting
- **837i (institutional medical claims)**  
- **837p (professional medial claims)**
- **834 (Enrollment)**
- **835 (Remittance / payment on claims)**

[Data dictionaries available](/doc/)

### Thanks to contributions from our partners at [CitiusTech](https://www.citiustech.com/) 834 is now supported along with additional insights from 837s. 

### Thanks to recommendations from our partners at [V4C](https://www.v4c.ai/) for further contributions and extending the functionality of the parser. 

# Install

```python
pip install git+https://github.com/databricks-industry-solutions/x12-edi-parser
```

# Run 

### 837i and 837p sample data in Spark

> [!NOTE]
> All types of EDI formats can be procesed with the same code below. However, it is recommended to build dataframes and save by resource type (837i/837p, 835, 834) to avoid confusion over empty values on a row.

#### Using mapInArrow (recommended, fastest processing)

mapInArrow() is wrapped [here](./databricksx12/hls/mapinarrow_functions.py). Two functions are available:

| Function | Best For | Output | 2GB Risk |
|----------|----------|--------|----------|
| `from_edi` | Small/medium files (<100MB EDI) | 1 row per file, then use Spark `explode()` | ⚠️ Possible if JSON >2GB |
| `from_edi_exploded` | Large files or many claims | 1 row per claim (no `explode()` needed) | ✅ Safe (files are ~5KB each) |

> [!WARNING]
> Apache Arrow has a **2GB limit per string value**. When processing very large EDI files, the parsed JSON can exceed this limit and cause: `Cannot grow BufferHolder by size X because the size after growing exceeds size limitation 2147483632`. Use `from_edi_exploded` for large files.

##### Input DataFrame

| Name | Description/Purpose |
|------|---------------------|
| `value` | **Required**. A string column containing the raw EDI file content. |
| `pk` | **Optional**. A primary key column (string, integer, etc.) to track lineage or file metadata. If provided, it is preserved in the output. |

##### Option 1: `from_edi` (default, faster for small/medium files)

Returns one row per EDI file with nested JSON. Use `flatten_edi()` to explode to rows per claim.

```python
from ember.hls.mapinarrow_functions import from_edi, get_output_schema, flatten_edi

# Assuming 'df' is your dataframe with 'value' (and optional 'pk') columns
# 1. Check to make sure you're using the entire cluster
if df.rdd.getNumPartitions() < spark.sparkContext.defaultParallelism:
    print("Warning: you're not using the entire cluster. Repartitioning to increase parallelism")
    df = df.repartition(spark.sparkContext.defaultParallelism * 5)

# 2. Parse EDI content using mapInArrow
result_df = df.mapInArrow(
    lambda batches: from_edi(batches, include_original_edi_content=False),
    schema=get_output_schema(include_original_edi_content=False)
)

# 3. Flatten the JSON structure (explodes to one row per claim)
final_df = flatten_edi(result_df, spark)

# 4. Save the content 
final_df.write.mode("append").saveAsTable("...")
```

> [!TIP]
> **Performance Optimization**: By default, `include_original_edi_content=False` omits the original EDI content from the output, which:
> - **Significantly reduces memory usage** (EDI files can be 100KB-several MB per record)
> - **Prevents Arrow's 2GB buffer limit errors** on large files
> - **Improves processing speed** by reducing data transfer between workers
> 
> Only set `include_original_edi_content=True` if you need the original EDI for validation, auditing, or reprocessing purposes.

##### Option 2: `from_edi_exploded` (safe for very large files)

Returns one row per claim directly—avoids the 2GB limit entirely since individual claims are typically 1-10KB. No additional `flatten_edi()` call needed.

```python
from ember.hls.mapinarrow_functions import from_edi_exploded, get_exploded_schema
from pyspark.sql.functions import from_json, col

# Parse EDI and explode to one row per claim in a single step
result_df = df.mapInArrow(
    from_edi_exploded,
    schema=get_exploded_schema()
)
# Output: pk, edi_json (same structure as from_edi but with single claim per row)
# NOTE: Do NOT use flatten_edi() here - data is already one row per claim

row_map = lambda x: {**{'pk': x.pk}, **{'edi_json': json.loads(x.edi_json)}}
final_df = spark.read.json(result_df.rdd.map(row_map))

# Save the content
final_df.write.mode("append").saveAsTable("...")
```

> [!NOTE]  
> **When to use `from_edi_exploded`:**
> - EDI files larger than ~100MB
> - Files with 50,000+ claims
> - When you see Arrow buffer errors
> - When memory pressure is a concern
>
> **Trade-off**: `from_edi` + Spark `flatten_edi()` is slightly faster for small files because Spark's native explode is optimized.

##### First Output (`result_df`). Also see the (html notebook)[https://databricks-industry-solutions.github.io/x12-edi-parser/#x12-edi-parser_1.html] for sample output values

| Name | Description/Purpose |
|------|---------------------|
| `pk` | The primary key from the input DataFrame, preserved for lineage. |
| `edi_content` | **Optional**. The original raw EDI content for the file. Only included if `include_original_edi_content=True`. Omitting this column significantly reduces memory usage and helps avoid Arrow's 2GB buffer limit for large files. |
| `edi_json` | A String representation of the parsed content to be consumed by spark.read.json  |

##### Final Output (`final_df`)

| Name | Description/Purpose |
|------|---------------------|
| `pk` | The primary key from the input DataFrame, preserved for lineage. |
| `edi_content` | **Optional**. The original raw EDI content for the file. Only included if `include_original_edi_content=True` was used in the previous step. |
| `EDI.*` | Struct containing metadata about the EDI ISA Segment |
| `FunctionalGroup.*` | Struct containing metadata about the EDI Functional Group (e.g., GS segment details). |
| `Transaction.*` | Struct containing metadata about the specific EDI Transaction (e.g., ST segment details). |
| `*` | The fully parsed and structured view of claims information (837, 835, 834). One row per claim. See the data dictionaries under "doc" to see all fields parsed out |

Atlernatively build final_df from the result_df using scala (avoid python serialization on jvm)
```scala
import spark.implicits._

//assuming result_df saved off to a table first
val result_df = spark.read.table("...")

// Option 1: Using spark.read.json with RDD (direct translation)
val jsonRdd = result_df.select("pk", "edi_json").rdd.map { row =>
  val pk = row.getAs[String]("pk")
  val ediJson = row.getAs[String]("edi_json")
  s"""{"pk":"$pk","edi_json":$ediJson}"""
}

val final_df = spark.read.json(jsonRdd.toDS())
final_df.write.mode("append").saveAsTable("...")
```

#### Splitting with RDDs (not recommended)

```python
from ember import *
from ember.hls import *
#This class manages how the different formats are parsed together
from ember.hls.healthcare import HealthcareManager as hm
import json, itertools
from pyspark.sql.functions import input_file_name

df = spark.read.text("sampledata/837/*txt", wholetext = True)

rdd = (
df.withColumn("filename", input_file_name()).rdd #convert to rdd
.map(lambda row: (row.filename, EDI(row.value))) #parse as an EDI format
.flatMap(lambda edi: hm.flatten(edi[1], filename = edi[0])) #extract out healthcare specific groupings, one row per claim/remittance/enrollment etc
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

### Sample Output

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


## Small sampling of data 

```python
from ember import *
from ember.hls import *
from ember.hls.healthcare import HealthcareManager as hm
import json

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

## Other issues to be aware of

1. Legacy EDI format 4010 not supported (deprecated in 2012 in favor of 5010). This can be determined by data in the ISA (first segment) and results in no claim content able to be found

2. File encodings: not all EDI transactions are in UTF-8 format which is the default when running spark.read.text. The expected encodings for spark can be set with spark.read.option('encoding', '<your encoding>').text(...). Other encodings include 'US-ASCII', 'ISO-8859-1', 'utf-8-sig', 'utf-16be', etc. 

## Project support 

Please note the code in this project is provided for your exploration only, and are not formally supported by Databricks with Service Level Agreements (SLAs). They are provided AS-IS and we do not make any guarantees of any kind. Please do not submit a support ticket relating to any issues arising from the use of these projects. The source in this project is provided subject to the Databricks [License](./LICENSE). All included or referenced third party libraries are subject to the licenses set forth below.

Any issues discovered through the use of this project should be filed as GitHub Issues on the Repo. They will be reviewed as time permits, but there are no formal SLAs for support. 
