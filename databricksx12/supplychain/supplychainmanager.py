from ember.edi import EDIManager
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
        "832": PurchaseOrder,
        "810": Invoice
    }

    @classmethod
    def from_transaction(cls, transaction: Transaction):
        """

        Parses a transaction using the appropriate supply chain transaction class.
        """
        transaction_set_code = transaction.transaction_set_code
        parser_class = cls.TRANSACTION_SET_MAPPING.get(transaction_set_code)

        if not parser_class:
            # You can choose to raise an error or return None if the
            # transaction type is not supported.
            return None

        """
        TODO determine what granularity is needed (e.g. 1 row per INV seg)
        e.g. row identifiers https://github.com/databricks-industry-solutions/x12-edi-parser/blob/main/databricksx12/hls/healthcare.py#L94-L101
        TODO pass in how these classes accept data in their respective  __init__() methods
        e.g. a very complex healthcare example https://github.com/databricks-industry-solutions/x12-edi-parser/blob/main/databricksx12/hls/claim.py#L32-L39
        TODO call the classes build() method to get the json output. 
        e.g. calling build https://github.com/databricks-industry-solutions/x12-edi-parser/blob/main/databricksx12/hls/healthcare.py#L115
        """
        

    @classmethod
    def flatten(cls, edi, *args, **kwargs):
        """
        Flattens an entire EDI file into a list of structured
        supply chain documents. This method is designed to integrate
        seamlessly with Spark RDDs.

        Each item in the returned list is a dictionary containing metadata
        from the EDI structure and the parsed transaction document.
        """
        return [
            {
                **kwargs,
                'EDI': cls.class_metadata(edi),
                'FunctionalGroup': cls.class_metadata(fg),
                'Transaction': cls.class_metadata(trnx),
                'Document': doc.to_dict() if doc else None,
            }
            for fg in edi.functional_segments()
            for trnx in fg.transaction_segments()
            if (doc := cls.from_transaction(trnx)) is not None
        ]
