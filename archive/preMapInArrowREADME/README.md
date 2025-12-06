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

# Install

```python
pip install git+https://github.com/databricks-industry-solutions/x12-edi-parser
```

# Run 

### 837i and 837p sample data in Spark

> [!NOTE]
> All types of EDI formats can be procesed with the same code below. However, it is recommended to build dataframes and save by resource type (837i/837p, 835, 834) to avoid confusion over empty values on a row.


#### Using mapInArrow (recommended, fastest processing)

```python
from ember import *
from ember.hls import *
#This class manages how the different formats are parsed together
from ember.hls.healthcare import HealthcareManager as hm
import json, itertools
from typing import Iterator


# operates over a partition of data
# @param assumes DF column name "value" has EDI string data
def process_batch(batch): 
  return [json.dumps(d) for d in itertools.chain(*map(process_edi_string, batch.column("value")))]

#row processing function, converts String to json() data of claims info
def process_edi_string(edi_string): 
  try:
    return [claim.to_json() for claim in hm.from_edi(EDI(edi_string.as_py(), strict_transactions=False))]
  except Exception as e:
    return [{"is_error": "true", "error": str(e), "error_data": edi_string.as_py()}]

# Operates over multiple batches in a partition
def process_edi_batches_to_json(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
    # The function receives an ITERATOR of batches, so we loop through it
    all_json_strings_from_batch = itertools.chain(*map(process_batch,batches))
    # After processing all rows, if we have results, create and yield a new batch
    if all_json_strings_from_batch:
        json_array = pa.array(all_json_strings_from_batch, type=pa.string())
        yield pa.RecordBatch.from_arrays([json_array], names=['edi_json'])

#running with a DF      
#df = ... column name "value" contains raw string of  EDI file
df.mapInArrow(process_edi_batches_to_json, StructType([StructField("edi_json", StringType(), True)]))

#reuse result 
result_df.cache()

#Claim count extracted from data
result_df.count()

#Save off parsed information
claims = spark.read.json(result_df.rdd.map(lambda x: x.edi_json))

#claims.write.mode(...).saveAsTable()
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


## Error trapping and restartability 

Sharing a sample of how to build error handling, transparency, and restartability using EDI over RDDs

```python 
#helper class to store input/output information from each stage 
class ProcessingResult:
    """Container for processing results with error handling"""   
    def __init__(self, run_id, success, data = None, 
                 error_message = "None", error_func = "None", 
                 processing_timestamp = None):
        self.run_id = run_id
        self.success = success
        self.data = data
        self.error_message = error_message
        self.error_func = error_func
        self.processing_timestamp = processing_timestamp or [datetime.now().isoformat()] #track timestamps for each step of processing
    
    def to_dict(self):
        """Convert to dictionary for DataFrame serialization"""
        return {
            "run_id": self.run_id,
            "success": self.success,
            "error_message": self.error_message,
            "error_func": self.error_func,
            "processing_timestamp": self.processing_timestamp
        }
    #for flatMap() calls
    def __iter__(self): 
        if not isinstance(self.data, list):
            yield self
        else:
            for d in self.data:
                yield ProcessingResult(self.run_id, self.success, d, self.error_message, self.error_func, self.processing_timestamp)
    def to_row(self) -> Row:
        """Convert to PySpark Row"""
        return Row(**self.to_dict())

#function to consistently execute on each stage 
def process_edi(processing_result, func, **xargs):
    if not processing_result.success: #if a command has failed return immediately
        return processing_result 
    else:
        try:
            return ProcessingResult(
                run_id = processing_result.run_id,
                success = True,
                data = func(processing_result.data, **xargs),
                error_message = "None",
                error_func = "None",
                processing_timestamp = processing_result.processing_timestamp + [(str(func) + ":" + datetime.now().isoformat())]
            )
        
        except Exception as e:
            error_message = str(e)
            stack_trace = traceback.format_exc()
            
            return ProcessingResult(
                run_id = processing_result.run_id,
                success = False,
                data = None,
                error_message = f"{error_message}\nStack trace: {stack_trace}",
                error_func = str(func),
                processing_timestamp = datetime.now().isoformat()
            )
#sample execution
df = ...
result = (
    df.
        rdd.
        map(lambda row: #build EDI
          process_edi(
            ProcessingResult(run_id=<distinct run identifier/filename>
                ,success=True,
                data=row.value,  #column name of EDI data in df
                error_message=None, 
                error_func=None), EDI, strict_transactions=False)).
        flatMap(lambda pr: process_edi(pr, hm.flatten)). #build Healthcare specific configurations
        repartition(<# of cluster CPUs or higher>).
        map(lambda pr: process_edi(pr, hm.flatten_to_json)). #parse data 
        map(lambda pr: process_edi(pr, json.dumps)) #dump as json format
)

claims_data = spark.read.json((result.filter(lambda x: x.success).map(lambda x: x.data)))
run_metadata = result.map(lambda x: x.to_row()).toDF()

```


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


## Project support 

Please note the code in this project is provided for your exploration only, and are not formally supported by Databricks with Service Level Agreements (SLAs). They are provided AS-IS and we do not make any guarantees of any kind. Please do not submit a support ticket relating to any issues arising from the use of these projects. The source in this project is provided subject to the Databricks [License](./LICENSE). All included or referenced third party libraries are subject to the licenses set forth below.

Any issues discovered through the use of this project should be filed as GitHub Issues on the Repo. They will be reviewed as time permits, but there are no formal SLAs for support. 
