from ember.edi import EDIManager
from ember.format import AnsiX12Delim
from ember.supplychain import *
# Import other supply chain transaction classes here as they are created
# from .invoice import Invoice_810
# from .ship_notice import ShipNotice_856


class SupplyChainManager(EDIManager):
    """
    Manages the parsing of various supply chain EDI transactions.

    This class uses a mapping to dynamically select the correct parser
    for a given EDI transaction type and orchestrates the parsing process.
    """

    # Maps transaction set codes to their corresponding parser classes.
    # The transaction_type is derived from the GS01 and ST01 segments.
    TRANSACTION_SET_MAPPING = {
        "832": ProductCatalog,
        "810": Invoice
    }

    @classmethod
    def determine_transaction_type(cls, gs_segment): 
        if gs_segment.element(1) == "IN":
            return cls.TRANSACTION_SET_MAPPING.get("810")
        raise Exception("No transaction type available for GS segment " + gs_segment.data)


    #
    # Convert a single 
    #
    @classmethod
    def from_transaction(cls, trnx):
        return list(SupplyBuilder(cls.determine_transaction_type(trnx.gs_segment),
                            [x for x in trnx.data if x._name not in ['ST', 'SE']], trnx.format_cls).build())


    #
    # Convert all data to json data
    #
    @classmethod
    def to_json(cls, edi):
        return {
            **EDIManager.class_metadata(edi),
            'FunctionalGroup': [
                {
                    **EDIManager.class_metadata(fg),
                    'Transactions': [
                        {
                            **EDIManager.class_metadata(trnx),
                            'Supply': [s.to_json() for s in cls.from_transaction(trnx)]
                        } for trnx in fg.transaction_segments()]
                } for fg in edi.functional_segments()] 
        }

class SupplyBuilder():

    def __init__(self, trnx_type_cls, trnx_data, delim_cls=AnsiX12Delim):
        self.data = trnx_data
        self.format_cls = delim_cls
        self.trnx_cls = trnx_type_cls

    #
    # iterate through the transaction and yield relevant rows
    #
    def build(self):
        if self.trnx_cls.NAME == "810":
            for i, seg in enumerate(self.data):
                if seg._name == "BIG": #one row per BIG segment
                    yield self.trnx_cls(seg)
        else:
            Exception("transaction class not implemented yet: " + str(self.trnx_type_cls))