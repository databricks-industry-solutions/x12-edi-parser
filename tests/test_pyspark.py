from .test_spark_base import *
from ember.edi import *
from ember.hls import *
import json

class TestPyspark(PysparkBaseTest):

    def test_transaction_count(self):
        df = self.spark.read.text("sampledata/837/*txt", wholetext=True)
        data = (df.rdd
                .map(lambda x: x.asDict().get("value"))
                .map(lambda x: EDI(x))
                .map(lambda x: {"transaction_count": x.num_transactions()})
                ).toDF()
        assert ( data.count() == 5) #5 rows
        assert ( data.select(data.transaction_count).groupBy().sum().collect()[0]["sum(transaction_count)"] == 9) #8 ST/SE transactions

    def test_835_plbs(self):
        df = self.spark.read.text("sampledata/835/*plb*txt", wholetext=True)
        rdd = (
            df.rdd
            .map(lambda row: EDI(row.value))
            .map(lambda edi: HealthcareManager.flatten(edi))
            .flatMap(lambda x: x)
        )
        claims_rdd = (rdd
            .map(lambda x: HealthcareManager.flatten_to_json(x))
            .map(lambda x: json.dumps(x))
        )

        claims = self.spark.read.json(claims_rdd)
        result = claims.select("provider_adjustments").take(6)
        assert(len(result) == 6)
        len([x.asDict() for x in result if x.asDict()['provider_adjustments'] == []])
        


if __name__ == '__main__':
    unittest.main()
