from test_spark_base import *
from databricksx12.edi import *

class TestPyspark(PysparkBaseTest):

    df = spark.read.text("sampledata/837/CC_837I_EDI.txt", wholetext=True)


    def test_spark_df(self):
        df.withColumn(
