from databricksx12.edi import *

#
# Base claim class
#


class Claim837i(Claim):

    NAME = "837I"

# Format of 837P https://www.dhs.wisconsin.gov/publications/p0/p00265.pdf


class Claim837p(Claim):

    NAME = "837P"
