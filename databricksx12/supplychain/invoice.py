

class Invoice():
    NAME = "810"

    #
    # Simplicity just using one segment for this class. 
    #
    def __init__(self, inv_seg):
        self.inv = inv_seg
        self.inv_info = None

    """
    TODO Given the segments/loops from init, build key/value pairs
    """
    def build(self):
        self.inv_info = {
            "invoice_date": self.inv.element(1),
            "invoice_number": self.inv.element(2),
            "po_date": self.inv.element(3),
            "po_number": self.inv.element(4)
        } 

    def to_json(self):
        if self.inv_info is None:
            self.build()
        return self.inv_info
