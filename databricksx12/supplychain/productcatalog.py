class ProductCatalog:
    NAME = "832"

    def __init__(self):
        """
        TODO: What segments / loops get passed in here on initial creation? 
        """
        pass

    """
    TODO Given the segments/loops from init, build key/value pairs
    """
    def build(self):
        #self.po_number = self._first(self.data, "BEG").element(3)
        #self.po_date = self._first(self.data, "BEG").element(5)
        # Add more parsing logic here for line items, addresses, etc.
        pass

    """
    TODO How to present & group key/value pairs back to Spark
    """
    def to_dict(self):
        return {
            "productname...": "self.productname",
            "productsku...": "self.productsku"
        }

        