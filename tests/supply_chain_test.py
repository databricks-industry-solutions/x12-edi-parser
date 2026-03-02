import unittest, json
from .test_spark_base import *
from ember.edi import EDI
from ember.supplychain.supplychainmanager import SupplyChainManager


class TestSupplyChain(PysparkBaseTest):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.sample_810 = EDI(open("sampledata/810/sample_810.txt", "rb").read().decode("utf-8"))

    def test_810_to_json(self):
        result = SupplyChainManager.to_json(self.sample_810)
        assert("FunctionalGroup" in result)
        assert(len(result["FunctionalGroup"]) == 1)
        fg = result["FunctionalGroup"][0]
        assert(len(fg["Transactions"]) == 1)
        trnx = fg["Transactions"][0]
        assert(len(trnx["Supply"]) == 1)
        supply = trnx["Supply"][0]
        assert(supply["invoice_date"] == "20250213")
        assert(supply["invoice_number"] == "INV0001")
        print(json.dumps(result, indent=2))


if __name__ == '__main__':
    unittest.main()
