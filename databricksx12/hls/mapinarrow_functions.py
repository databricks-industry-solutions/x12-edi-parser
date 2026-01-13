"""
MapInArrow functions for efficient EDI parsing in Apache Spark.

Usage Examples:
    
    # Default: Without original EDI content (recommended for large files)
    from databricksx12.hls.mapinarrow_functions import from_edi, get_output_schema
    
    result_df = df.mapInArrow(
        lambda batches: from_edi(batches, include_original_edi_content=False),
        schema=get_output_schema(include_original_edi_content=False)
    )
    
    # With original EDI content (use only if needed)
    result_df = df.mapInArrow(
        lambda batches: from_edi(batches, include_original_edi_content=True),
        schema=get_output_schema(include_original_edi_content=True)
    )
"""

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
# Helper functions to execute efficiently in Spark
#

#
# Function to run on mapInArrow(from_edi...)
#
def from_edi(batches: Iterator[pa.RecordBatch], include_original_edi_content: bool = False) -> Iterator[pa.RecordBatch]:
    """
    Parse EDI content and return structured data.
    
    Args:
        batches: Iterator of PyArrow RecordBatches containing EDI data
        include_original_edi_content: If True, includes the original EDI string in output.
                                      Default is False to reduce memory usage and avoid
                                      Arrow's 2GB buffer limit on large files.
    
    Returns:
        Iterator of PyArrow RecordBatches with parsed EDI data
    """
    def safe_parse_edi(edi_string):
        """Parse EDI string with error handling to prevent crashes"""
        try:
            if not edi_string or len(edi_string.strip()) == 0:
                return json.dumps({"error": "Empty EDI string"})
            
            edi_obj = EDI(edi_string, strict_transactions=False)
            result = hm.to_json(edi_obj)
            json_str = json.dumps(result)
            
            # Check if the JSON string is too large (approaching 2GB limit)
            # Arrow has a 2GB limit per string, so warn at 2GB
            if len(json_str) > 2_000_000_000:  # 2GB
                return json.dumps({
                    "error": "EDI JSON too large for mapInArrow",
                    "size_bytes": len(json_str),
                    "edi_preview": edi_string[:1000]
                })
            
            return json_str
        except Exception as e:
            return json.dumps({
                "error": f"Failed to parse EDI: {type(e).__name__}",
                "message": str(e),
                "edi_preview": edi_string[:1000] if edi_string else ""
            })
    
    for batch in batches:
        pk_column = batch.column("pk") if "pk" in batch.schema.names else pa.array([""] * batch.num_rows, type=pa.string())
        value_column = batch.column("value")
        
        # Process EDI strings with error handling
        edi_values = value_column.to_pylist()
        json_results = [safe_parse_edi(edi_string) for edi_string in edi_values]
        
        # Build columns and names based on include_original_edi_content parameter
        columns = [pk_column, pa.array(json_results, type=pa.string())]
        names = ['pk', 'edi_json']
        
        if include_original_edi_content:
            columns.insert(1, value_column)  # Insert between pk and edi_json
            names.insert(1, 'edi_content')
        
        yield pa.RecordBatch.from_arrays(columns, names=names)


# Helper function to get the appropriate schema based on configuration
def get_output_schema(include_original_edi_content: bool = False) -> StructType:
    """
    Returns the output schema for from_edi function.
    
    Args:
        include_original_edi_content: If True, includes edi_content column in schema
    
    Returns:
        StructType schema for Spark
    """
    if include_original_edi_content:
        return StructType([
            StructField("pk", StringType(), True),
            StructField("edi_content", StringType(), True),
            StructField("edi_json", StringType(), True)
        ])
    else:
        return StructType([
            StructField("pk", StringType(), True),
            StructField("edi_json", StringType(), True)
        ])


# Default schema (without original EDI content for better performance)
output_schema = get_output_schema(include_original_edi_content=False)

#
# Accept output from_edi() result and produce a json_df that can be saved as a table
#
def flatten_edi(from_edi_df, spark): 
    """
    Flatten the EDI JSON output into a table structure.
    
    Args:
        from_edi_df: DataFrame output from from_edi function
        spark: SparkSession
    
    Returns:
        Flattened DataFrame with exploded claims
    """
    # Check if edi_content column exists
    has_edi_content = "edi_content" in from_edi_df.columns
    
    # Build the row mapping based on available columns
    if has_edi_content:
        row_map = lambda x: {**{'pk': x.pk}, **{'edi_content': x.edi_content}, **{'edi_json': json.loads(x.edi_json)}}
    else:
        row_map = lambda x: {**{'pk': x.pk}, **{'edi_json': json.loads(x.edi_json)}}
    
    # Build select columns based on available columns
    select_cols = ["pk"]
    if has_edi_content:
        select_cols.append("edi_content")
    
    select_cols.extend([
        "`edi_json`.`EDI.control_number`",  
        "`edi_json`.`EDI.date`", 
        "`edi_json`.`EDI.recipient_qualifier_id`", 
        "`edi_json`.`EDI.sender_qualifier_id`", 
        "`edi_json`.`EDI.standard_version`", 
        "`edi_json`.`EDI.time`", 
        "FunctionalGroup.*", 
        "Transaction.*", 
        "Claim.*"
    ])
    
    return (
        spark.read.json(from_edi_df.rdd.map(row_map))
            .withColumn("FunctionalGroup", explode(col("edi_json.FunctionalGroup")))
            .withColumn("Transaction", explode(col("FunctionalGroup.Transactions")))
            .withColumn("Claim", explode(col("Transaction.Claims")))
            .select(*select_cols)
            .drop("FunctionalGroups", "Transactions", "Claims")
    )


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


