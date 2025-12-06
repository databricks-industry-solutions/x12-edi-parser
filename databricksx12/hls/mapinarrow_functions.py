import pyarrow as pa
from ember.hls.healthcare import HealthcareManager as hm
from ember import *
from ember.hls import *
import json, itertools
from typing import Iterator
from pyspark.sql import Row
from pyspark.sql.functions import explode, col
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, MapType, ArrayType

#
# TODO
#  (2) add in _extract_segments column of data
#  (3) update readme
#

#
# Function to run on mapInArrow(from_edi...)
#
def from_edi(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
    return map(lambda batch: pa.RecordBatch.from_arrays(
        [
            # Column 1: an optinal PK column if specified
            # Column 2: The original EDI content (reusing the input array)
            batch.column("pk") if "pk" in batch.schema.names else pa.array([""] * batch.num_rows, type=pa.string()),
            batch.column("value"),
            # Column 3: The parsed JSON data (calculated as before)
            pa.array(
                map(lambda edi_string: 
                    json.dumps(hm.to_json(EDI(edi_string, strict_transactions=False))),
                    batch.column("value").to_pylist()
                ), type=pa.string()
            )
        ], 
        names=['pk', 'edi_content', 'edi_json']
    ), batches)


output_schema = StructType([
    StructField("pk", StringType(), True),
    StructField("edi_content", StringType(), True),
    StructField("edi_json", StringType(), True)
])

#
# Accept output from_edi() result and produce a json_df that can be saved as a table
#
def flatten_edi(from_edi_df): 
   return (
    spark.read.json(
        result_df.rdd.map(lambda x: {**{'pk': x.pk}, **{'edi_content': x.edi_content}, **{'edi_json': json.loads(x.edi_json)}}))
            .withColumn("FunctionalGroup", explode(col("edi_json.FunctionalGroup")))
            .withColumn("Transaction", explode(col("FunctionalGroup.Transactions")))
            .withColumn("Claim", explode(col("Transaction.Claims")))
    ).select("pk", 
             "edi_content", 
             "`edi_json`.`EDI.control_number`",  
             "`edi_json`.`EDI.date`", 
             "`edi_json`.`EDI.recipient_qualifier_id`", 
             "`edi_json`.`EDI.sender_qualifier_id`", 
             "`edi_json`.`EDI.standard_version`", 
             "`edi_json`.`EDI.time`", 
             "FunctionalGroup.*", 
             "Transaction.*", 
             "Claim.*").drop("FunctionalGroups", "Transactions", "Claims")


"""
def read_file(f): 
  with open(f.replace("dbfs:", "/dbfs"), "rb") as file:
    data = file.read().decode("utf-8")
    file.close()
    return Row(**{"value": data})
  return ""

rdd = sc.parallelize(file_path_list)

df = rdd.map(lambda x: read_file(x)).toDF()
df.show()

df.mapInArrow(from_edi, output_schema)
"""


