from databricksx12.edi import EDI, AnsiX12Delim, Segment
from databricksx12.hls.loop import Loop
from databricksx12.hls.identities import *
from typing import List, Dict
from collections import defaultdict


#
# Base claim builder (transaction -> 1 or more claims)
#


class ClaimBuilder(EDI):
    #
    # Given claim type (837i, 837p, etc), segments, and delim class, build claim level classes
    #
    def __init__(self, trnx_type_cls, trnx_data, delim_cls=AnsiX12Delim):
        self.data = trnx_data
        self.format_cls = delim_cls
        self.trnx_cls = trnx_type_cls
        self.loop = Loop(trnx_data)

    #
    # Builds a claim object from
    #
    # @param clm_segment - the claim segment of claim to build
    # @param idx - the index of the claim segment in the data
    #
    #  @return the class containing the relevent claim information
    #
    def build_claim(self, clm_segment, idx):
        return self.trnx_cls(
            sender_receiver_loop=self.get_submitter_receiver_loop(idx),
            billing_loop=self.loop.get_loop_segments(idx, "2000A"),
            subscriber_loop=self.loop.get_loop_segments(idx, "2000B"),
            patient_loop=self.loop.get_loop_segments(idx, "2000C"),
            claim_loop=self.get_claim_loop(idx),
            sl_loop=self.get_service_line_loop(idx),  # service line loop
        )


        
    #
    # https://datainsight.health/edi/payments/dollars-separate/
    #  trx_header_loop = 0000
    #  payer_loop = 1000A
    #  payee_loop = 1000B
    #  clm_payment_loop = 2100
    #  srv_payment_loop = 2110
    def build_remittance(self, pay_segment, idx):
        return self.trnx_cls(trx_header_loop = self.data[0:self.index_of_segment(self.data, "N1")]
                             ,payer_loop = self.data[self.index_of_segment(self.data, "N1"):self.index_of_segment(self.data, "N1", self.index_of_segment(self.data, "N1")+1)]
                             ,payee_loop = self.data[self.index_of_segment(self.data, "N1", self.index_of_segment(self.data, "N1")+1): self.index_of_segment(self.data, "LX")]
                             ,clm_loop = self.data[idx:min(
                                 list(filter(lambda x: x > 0, [self.index_of_segment(self.data, "LX", idx+1), 
                                  self.index_of_segment(self.data, "CLP", idx+1),
                                  self.index_of_segment(self.data, "SE", idx+1),
                                  len(self.data)])
                                ))]
                             ,trx_summary_loop = self.data[max(0,
                                self.last_index_of_segment(self.data, "LX"),
                                self.last_index_of_segment(self.data, "CLP"),
                                self.last_index_of_segment(self.data, "SVC")
                             ):]
                            )

    def build_enrollment(self, pay_segment, idx):
        return self.trnx_cls(
            enrollment_member = self.data[self.index_of_segment(self.data, "INS"): self.index_of_segment(self.data, "SE")],
            health_plan_loop=self.data[self.index_of_segment(self.data, "HD"): self.last_index_of_segment(self.data, "DTP")+1]
        )
    #
    # Determine claim loop: starts at the clm index and ends at LX segment, or CLM segment, or end of data
    #
    def get_claim_loop(self, clm_idx):
        sl_start_indexes = list(map(lambda x: x[0], filter(lambda x: x[0] > clm_idx, self.segments_by_name_index("LX"))))
        clm_indexes = list(map(lambda x: x[0], filter(lambda x: x[0] > clm_idx, self.segments_by_name_index("CLM"))))

        if sl_start_indexes:
            clm_end_idx = min(sl_start_indexes)
        elif clm_indexes:
            clm_end_idx = min(clm_indexes + [len(self.data)])
        else:
            clm_end_idx = len(self.data)
        
        return self.data[clm_idx:clm_end_idx]

    #
    # fetch the indices of LX and CLM segments that are beyond the current clm index
    #
    def get_service_line_loop(self, clm_idx):
        sl_start_indexes = list(map(lambda x: x[0], filter(lambda x: x[0] > clm_idx, self.segments_by_name_index("LX"))))
        tx_end_indexes = list(map(lambda x: x[0], filter(lambda x: x[0] > clm_idx, self.segments_by_name_index("SE"))))
        if sl_start_indexes:
            sl_end_idx = min(tx_end_indexes + [len(self.data)])
            return self.data[min(sl_start_indexes):sl_end_idx]
        return []

    def get_submitter_receiver_loop(self, clm_idx):
        bht_start_indexes = list(map(lambda x: x[0], filter(lambda x: x[0] < clm_idx, self.segments_by_name_index("BHT"))))
        bht_end_indexes = list(map(lambda x: x[0], filter(lambda x: x[0] < clm_idx and x[1].element(3) == '20', self.segments_by_name_index("HL"))))
        if bht_start_indexes:
            sub_rec_start_idx = max(bht_start_indexes)
            sub_rec_end_idx = max(bht_end_indexes)

            return self.data[sub_rec_start_idx:sub_rec_end_idx]
        return []


    #
    # Given transaction type, transaction segments, and delim info, build out claims in the transaction
    #  @return a list of Claim for each "clm" segment
    #
    def build(self):
        if self.trnx_cls.NAME in ['837I', '837P']:
            return [
                self.build_claim(seg, i) for i, seg in self.segments_by_name_index("CLM")
            ]
        elif self.trnx_cls.NAME == '835':
            return [
                self.build_remittance(seg, i) for i, seg in self.segments_by_name_index("CLP")
            ]
        elif self.trnx_cls.NAME == '834':
            return [
                self.build_enrollment(seg, i) for i, seg in self.segments_by_name_index("BGN")
            ]

#
# Base claim class
#

class MedicalClaim(EDI):

    def __init__(
        self,
        sender_receiver_loop: List = [],
        billing_loop: List = [],
        subscriber_loop: List = [],
        patient_loop: List = [],
        claim_loop: List = [],
        sl_loop: List = [], 
    ):
        self.sender_receiver_loop = sender_receiver_loop # extracted together
        self.billing_loop = billing_loop
        self.subscriber_loop = subscriber_loop
        self.patient_loop = patient_loop
        self.claim_loop = claim_loop
        self.sl_loop = sl_loop
        self.build()

    #
    # Return first segment found of name == name otherwise Segment.empty()
    #
    def _first(self, segments, name, start_index = 0):
        return ([x for x in segments[start_index:] if x.segment_name() == name][0]  if len([x for x in segments[start_index:] if x.segment_name() == name]) > 0 else Segment.empty())
        
    def _populate_providers(self):
        return {"billing": self._billing_provider()}
    
    def _billing_provider(self):
        return ProviderIdentity(nm1=self._first(self.billing_loop, "NM1"),
                                n3=self._first(self.billing_loop, "N3"),
                                n4=self._first(self.billing_loop, "N4"),
                                ref=self._first(self.billing_loop, "REF"))

    def _populate_diagnosis(self):
        return DiagnosisIdentity([x for x in self.claim_loop if x.segment_name() == "HI"])
    
    def _populate_submitter_loop(self) -> Dict[str, str]:
        return Submitter_Receiver_Identity(nm1=self._first([x for x in self.sender_receiver_loop if x.element(1) == "41"],"NM1"),
                                           per=self._first(self.sender_receiver_loop, "PER"))
    
    def _populate_receiver_loop(self) -> Dict[str, str]:
        return Submitter_Receiver_Identity(nm1=self._first([x for x in self.sender_receiver_loop if x.element(1) == "40"],"NM1"))

    def _populate_subscriber_loop(self):
        l = self.subscriber_loop[0:min(filter(lambda x: x!= -1, [self.index_of_segment(self.subscriber_loop, "CLM"), len(self.subscriber_loop)]))] #subset the subscriber loop before the CLM segment
        return PatientIdentity(
            nm1 = self._first(l, "NM1"),
            n3 = self._first(l, "N3"),
            n4 = self._first(l, "N4"),
            dmg = self._first(l, "DMG"),            
            pat = self._first(l, "PAT"),
            sbr = self._first(l, "SBR"),
            ref = self._first([x for x in l if x.element(1) == "EA"], "REF")
        )
    
    def _populate_patient_loop(self) -> Dict[str, str]:
        # Note - if this doesn't exist then it's the same as subscriber loop
        # 01 = Spouse; 18 = Self; 19 = Child; G8 = Other
        return self._populate_subscriber_loop() if self._first(self.subscriber_loop, "SBR").element(2) == "18" else PatientIdentity(
            nm1 = self._first(self.patient_loop, "NM1"),
            n3 = self._first(self.patient_loop, "N3"),
            n4 = self._first(self.patient_loop, "N4"),
            dmg = self._first(self.patient_loop, "DMG"),
            pat = self._first(self.patient_loop, "PAT"),
            sbr = self._first(self.patient_loop, "SBR"),
            ref = self._first([x for x in self.patient_loop if x.element(1) == "EA"], "REF"))
    
    def _populate_claim_loop(self):
        return ClaimIdentity(clm = self._first(self.claim_loop, "CLM"),
                             dtp = self.segments_by_name("DTP", data=self.claim_loop),
                             k3 = self._first(self.claim_loop, "K3"),
                             ref = self.segments_by_name("REF", data=self.claim_loop[:(len(self.claim_loop)-1 if (temp := [i for i, x in enumerate(self.claim_loop) if x.segment_name() == "NM1"]) == [] else temp[0]) ]), #ref up until loop 2310
                             amt = self.segments_by_name("AMT", data=self.claim_loop)) #ref up until loop 2310


                             

    def _populate_payer_info(self):
        return PayerIdentity(self._first([x for x in self.subscriber_loop if x.element(1) == "PR"], "NM1"))
    
    """
    Overall Asks
    - Coordination of Benefits flag -- > self.benefits_assign_flag in Claim Identity
    """
    def to_json(self):
        return {
            **{'submitter': self.submitter_info.to_dict()},
            **{'receiver': self.receiver_info.to_dict()},
            **{'subscriber': self.subscriber_info.to_dict()},
            **{'patient': self.patient_info.to_dict()},
            **{'payer': self.payer_info.to_dict()},
            **{'providers': {k:v.to_dict() for k,v in self.provider_info.items()}}, #returns a dictionary of k=provider type
            **{'claim_header': self.claim_info.to_dict()},
            **{'claim_lines': [x.to_dict() for x in self.sl_info]}, #List
            **{'diagnosis': self.diagnosis_info.to_dict()}
        }

    def _service_facility_provider(self):
        i, nm1 = ((-1,Segment.empty()) if (temp := [(i,x) for i, x in enumerate(self.claim_loop) if x.element(1) == "77" and x.segment_name() == "NM1"]) == [] else temp[0])
        return ProviderIdentity(nm1=nm1,
                                n3=self._first([x for x in self.claim_loop[i+1:i+3]],"N3"),
                                n4=self._first([x for x in self.claim_loop[i+1:i+3]],"N4"),
                                ref= Segment.empty())

    #
    # Returns each claim line as an array of segments that make up the claim line
    #
    def claim_lines(self):
        return list(map(lambda i: self.sl_loop[i[0]:i[1]],
                self._index_to_tuples([(i) for i,y in enumerate(self.sl_loop) if y.segment_name()=="LX"]+[len(self.sl_loop)])))

    def build(self) -> None:
        self.submitter_info = self._populate_submitter_loop()
        self.receiver_info = self._populate_receiver_loop()
        self.subscriber_info = self._populate_subscriber_loop()
        self.patient_info = (
            self._populate_subscriber_loop() if self.patient_loop == [] else self._populate_patient_loop()
        )
        self.sl_info =  self._populate_sl_loop()
        self.claim_info = self._populate_claim_loop()
        self.provider_info = self._populate_providers()
        self.diagnosis_info = self._populate_diagnosis()
        self.payer_info = self._populate_payer_info()

class Claim837i(MedicalClaim):

    NAME = "837I"

    # Format for 837I https://www.dhs.wisconsin.gov/publications/p0/p00266.pdf
    
    def _attending_provider(self):
        i, nm1 = ((-1,Segment.empty()) if (temp := [(i,x) for i, x in enumerate(self.claim_loop) if x.element(1) == "71" and x.segment_name() == "NM1"]) == [] else temp[0])
        return ProviderIdentity(nm1=nm1,
                                prv=self._first([x for x in self.claim_loop if x.element(1) == "AT"],"PRV"),
                                ref=(Segment.empty() if i == -1 else self._first(self.claim_loop[i:self.index_of_segment(self.claim_loop, "NM1", i+1)],  "REF")) )

    def _operating_provider(self):
        return ProviderIdentity(nm1=self._first([x for x in self.claim_loop if x.element(1) == "72"],"NM1"),
                                prv=self._first([x for x in self.claim_loop if x.element(1) == "OP"],"PRV"))

    def _other_provider(self):
        return ProviderIdentity(nm1=self._first([x for x in self.claim_loop if x.element(1) == "73"],"NM1"),
                                prv=self._first([x for x in self.claim_loop if x.element(1) == "OT"],"PRV")) 
      
    def _populate_providers(self):
        return {"billing": self._billing_provider(),
                "attending": self._attending_provider(),
                "operating": self._operating_provider(),
                "other": self._other_provider(),
                "service_facility": self._service_facility_provider()
                }

    def _populate_claim_loop(self):
        return ClaimIdentity(clm = self._first(self.claim_loop, "CLM"),
                             dtp = self.segments_by_name("DTP", data = self.claim_loop),
                             cl1 = self._first(self.claim_loop, "CL1"),
                             k3 = self._first(self.claim_loop, "K3"),
                             hi = self._first([x for x in self.claim_loop if x.segment_name() == "HI" and x.element(1).startswith("DR")], "HI"), #DRG_CD
                             ref = self.segments_by_name("REF", data=self.claim_loop[:(len(self.claim_loop)-1 if (temp := [i for i, x in enumerate(self.claim_loop) if x.segment_name() == "NM1"]) == [] else temp[0]) ]), #ref up until loop 2310 
                             amt = self.segments_by_name("AMT", data=self.claim_loop),
                             principal_hi = self._first([x for x in self.claim_loop if x.segment_name() == "HI" and x.element(1,0) == ("BBR")], "HI"),
                             other_hi = [x for x in self.claim_loop if x.segment_name() == "HI" and x.element(1,0) == ("BBQ")]
                             )

    def _populate_sl_loop(self, missing=""):
        return list(
            map(lambda s:
                ServiceLine.from_sv2(
                    sv2 = self._first(s, "SV2"),
                    lx = self._first(s, "LX"),
                    dtp = self.segments_by_name("DTP", data = s),
                    amt = self.segments_by_name("AMT", data=s)
                ),self.claim_lines()))

    
class Claim837p(MedicalClaim):

    NAME = "837P"
    # Format of 837P https://www.dhs.wisconsin.gov/publications/p0/p00265.pdf

    def _rendering_provider(self):
        return ProviderIdentity(nm1=self._first([x for x in self.claim_loop if x.element(1) == "82"],"NM1"),
                                prv=self._first([x for x in self.claim_loop if x.element(1) == "PE"],"PRV"))
    
    def _referring_provider(self):
        return ProviderIdentity(nm1=self._first([x for x in self.claim_loop if x.element(1) == "DN"],"NM1"),
                                prv=self._first([x for x in self.claim_loop if x.element(1) == "RF"],"PRV"))


    def _populate_providers(self):
        return {"billing": self._billing_provider(),
                "referring": self._referring_provider(),
                "servicing": (self._billing_provider() if self._rendering_provider() is None else self._rendering_provider()),
                "service_facility": self._service_facility_provider()
                }

    
    def _populate_sl_loop(self, missing=""):
        return list(
            map(lambda s:
                ServiceLine.from_sv1(
                    sv1 = self._first(s, "SV1"),
                    lx = self._first(s, "LX"),
                    dtp = self.segments_by_name("DTP", data=s),
                    amt = self.segments_by_name("AMT", data=s)
                ), self.claim_lines()))
