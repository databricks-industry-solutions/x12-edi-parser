from .test_spark_base import *
from .test_pyspark import *
from databricksx12.hls import *
from databricksx12 import *
import unittest, re
from os import listdir
from os.path import isfile, join

class TestIssues(PysparkBaseTest):

    #Pulling delimiters from ISA 
    def test_issue21(self):
        files = ['sampledata/837/' + f for f in listdir('sampledata/837') if isfile(join('sampledata/837', f))]
        assert(set(map(lambda f:
                 EDI.extract_delim(open(f, "rb").read().decode("utf-8")) == AnsiX12Delim
            , files)) == {True})

    #claim header level date/time and date qualifier codes
    def test_issue24(self):
        edi = EDI(open('sampledata/837/CC_837I_EDI.txt', "rb").read().decode("utf-8"))
        hm = HealthcareManager()
        data = hm.from_edi(edi)
        assert(len(data[0].to_json()['claim_header']['claim_dates']))
        assert([x['date_cd'] for x in data[0].to_json()['claim_header']['claim_dates']] == ['096', '434', '435'])
        assert([x['date'] for x in data[0].to_json()['claim_header']['claim_dates']] == ['0900', '20180628-20180702', '201806280800'])
        assert([x['date_format'] for x in data[0].to_json()['claim_header']['claim_dates']] == ['TM', 'RD8', 'DT'])


    
