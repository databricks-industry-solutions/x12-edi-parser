from databricksx12.edi import EDI, AnsiX12Delim, Segment
from databricksx12.hls.loop import Loop
from databricksx12.hls.identities import *
from typing import List, Dict
import functools
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
            ref = self._first(l, "REF")
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
            ref = self._first(self.patient_loop, "REF"))            
    
    def _populate_claim_loop(self):
        return ClaimIdentity(clm = self._first(self.claim_loop, "CLM"),
                             dtp = self.segments_by_name("DTP", data=self.claim_loop),
                             k3 = self._first(self.claim_loop, "K3"),
                             ref = self.segments_by_name("REF", data=self.claim_loop[:(len(self.claim_loop)-1 if (temp := [i for i, x in enumerate(self.claim_loop) if x.segment_name() == "NM1"]) == [] else temp[0]) ])) #ref up until loop 2310

                             

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

    #
    # Returns each claim line as an array of segments that make up the claim line
    #
    def claim_lines(self):
        return list(map(lambda i: self.sl_loop[i[0]:i[1]],
                self._index_to_tuples([(i) for i,y in enumerate(self.sl_loop) if y.segment_name()=="LX"]+[len(self.sl_loop)])))

    # not sure if this should be here or not, but you get the idea
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
        i, x = ((-1,Segment.empty()) if (temp := [(i,x) for i, x in enumerate(self.claim_loop) if x.element(1) == "71" and x.segment_name() == "NM1"]) == [] else temp[0])
        return ProviderIdentity(nm1=x,
                                prv=self._first([x for x in self.claim_loop if x.element(1) == "AT"],"PRV"),
                                ref=(Segment.empty() if i == -1 else self._first(self.claim_loop[i:self.index_of_segment(self.claim_loop, "NM1", i+1)],  "REF")) )

    def _operating_provider(self):
        return ProviderIdentity(nm1=self._first([x for x in self.claim_loop if x.element(1) == "72"],"NM1"),
                                prv=self._first([x for x in self.claim_loop if x.element(1) == "OP"],"PRV"))

    def _other_provider(self):
        return ProviderIdentity(nm1=self._first([x for x in self.claim_loop if x.element(1) == "73"],"NM1"),
                                prv=self._first([x for x in self.claim_loop if x.element(1) == "OT"],"PRV")) 

    def _facility_provider(self):
        return ProviderIdentity(nm1=self._first([x for x in self.claim_loop if x.element(1) == "FA"],"NM1")) # also in 837P but rare
        
    def _populate_providers(self):
        return {"billing": self._billing_provider(),
                "attending": self._attending_provider(),
                "operating": self._operating_provider(),
                "other": self._other_provider(),
                "facility": self._facility_provider()
                }

    def _populate_claim_loop(self):
        return ClaimIdentity(clm = self._first(self.claim_loop, "CLM"),
                             dtp = self.segments_by_name("DTP", data = self.claim_loop),
                             cl1 = self._first(self.claim_loop, "CL1"),
                             k3 = self._first(self.claim_loop, "K3"),
                             hi = self._first([x for x in self.claim_loop if x.segment_name() == "HI" and x.element(1).startswith("DR")], "HI"), #DRG_CD
                             ref = self.segments_by_name("REF", data=self.claim_loop[:(len(self.claim_loop)-1 if (temp := [i for i, x in enumerate(self.claim_loop) if x.segment_name() == "NM1"]) == [] else temp[0]) ])) #ref up until loop 2310 
    
    def _populate_sl_loop(self, missing=""):
        return list(
            map(lambda s:
                ServiceLine.from_sv2(
                    sv2 = self._first(s, "SV2"),
                    lx = self._first(s, "LX"),
                    dtp = self.segments_by_name("DTP", data = s)
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

    def _service_facility_provider(self):
        return ProviderIdentity(nm1=self._first([x for x in self.claim_loop if x.element(1) == "77"],"NM1"))

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
                    dtp = self.segments_by_name("DTP", data=s)
                ), self.claim_lines()))

#
# 835 payment information
#  https://datainsight.health/edi/payments/with-discount/
#
class Remittance(MedicalClaim):

    NAME = "835"
    
    def __init__(self,
                 trx_header_loop,
                 payer_loop,
                 payee_loop,
                 clm_loop,
                 trx_summary_loop):
        self.trx_header_loop = trx_header_loop
        self.payer_loop = payer_loop
        self.payee_loop = payee_loop
        self.clm_loop = clm_loop
        self.trx_summary_loop = trx_summary_loop
        self.build()

    def build(self):
        self.trx_header_info = self.populate_trx_loop()
        self.payer_info = self.populate_payer_loop()
        self.payee_info = self.populate_payee_loop()
        self.clm_info = self.populate_claim_loop()
        self.plb_info = self.populate_plb_loop()

    #
    # Assuming npi, date, then repeating <reason cd:id, adj amount>
    #
    def populate_plb_loop(self):
        return functools.reduce(lambda x, y: x+y, [
            [{
                'provider_adjustment_npi': p.element(1),
                'provider_adjustment_date': p.element(2),
                'provider_adjustment_reason_cd': p.element(i, 0),
                'provider_adjustment_id': p.element(i, 1),
                'provider_adjustment_amt': p.element(i+1)
            }
             for i in list(range(3,p.segment_len(), 2))]
            for p in self.segments_by_name("PLB", data=self.trx_summary_loop)], [])

    def populate_payer_loop(self):
        return {
            'entity_id_cd': self._first(self.payer_loop, "N1").element(1),
            'payer_name': self._first(self.payer_loop, "N1").element(2),
            'payer_street': self._first(self.payer_loop, "N3").element(1),
            'payer_city': self._first(self.payer_loop, "N4").element(1),
            'payer_state': self._first(self.payer_loop, "N4").element(2),
            'payer_zip': self._first(self.payer_loop, "N4").element(3),
            'payer_contact_info': [
                {
                    'payer_contact_name': c.element(2),
                    'payer_contact_function_cd': c.element(1),
                    'payer_contact_number': c.element(6),
                    'payer_email': c.element(4)
                }
                for c in self.segments_by_name("PER", data = self.payer_loop)],
            'payer_primary_id': self._first(self.payer_loop, "REF").element(1),
            'payer_secondary_id': self._first(self.payer_loop, "REF").element(2)
        }

    def populate_payee_loop(self):
        return {
            'payee_name': self._first(self.payee_loop, "N1").element(2),
            'payee_npi': self._first(self.payee_loop, "N1").element(3),
            'payee_id_cd': self._first(self.payee_loop, "N1").element(4),
            'payee_tax_id': self._first(self.payee_loop, "REF").element(2)
        }
    
    def populate_trx_loop(self):
        return {
            'transaction_handling_cd': self._first(self.trx_header_loop,"BPR").element(1),
            'monetary_amt': self._first(self.trx_header_loop,"BPR").element(2),
            'credit_debit_flag': self._first(self.trx_header_loop,"BPR").element(3),
            'payment_method_cd': self._first(self.trx_header_loop,"BPR").element(4),
            'payment_date': self._first(self.trx_header_loop,"BPR").element(16),
            'trace_type_cd': self._first(self.trx_header_loop,"TRN").element(1),
            'trace_reference_id': self._first(self.trx_header_loop,"TRN").element(2),            
            'trace_origin_company_id':  self._first(self.trx_header_loop,"TRN").element(3)
            }

    def populate_claim_loop(self):
        end_clp_index = ([i for i,z in enumerate([y.segment_name() for y in self.clm_loop[1:]]) if z == "CLP"] + [len(self.clm_loop)])[0]
        return {
            'claim_id': self._first(self.clm_loop,"CLP").element(1),
            'person_or_organization': self._populate_names(self.clm_loop[:end_clp_index]),
            'claim_status_cd': self._first(self.clm_loop,"CLP").element(2),
            'claim_chrg_amt': self._first(self.clm_loop,"CLP").element(3),
            'claim_pay_amt': self._first(self.clm_loop,"CLP").element(4),
            'patient_pay_amt': self._first(self.clm_loop,"CLP").element(5),
            'claim_filing_cd': self._first(self.clm_loop,"CLP").element(6),
            'payer_claim_id': self._first(self.clm_loop,"CLP").element(7),
            'type_of_bill_cd': self._first(self.clm_loop,"CLP").element(8),
            'claim_freq_cd': self._first(self.clm_loop,"CLP").element(9),
            'drg_cd': self._first(self.clm_loop,"CLP").element(11),
            'patient_entity_id_cd': self._first(self.clm_loop,"NM1").element(1),
            'entity_type_qualifier': self._first(self.clm_loop,"NM1").element(2),
            'patient_last_nm': self._first(self.clm_loop,"NM1").element(4),
            'patient_first_nm': self._first(self.clm_loop,"NM1").element(5),
            'id_code_qualifier': self._first(self.clm_loop,"NM1").element(8),
            'patient_id': self._first(self.clm_loop,"NM1").element(9),
            'clm_refs': [{'id_code_qualifier': x.element(1), 'id': x.element(2)}  for x in self.segments_by_name("REF", data=self.clm_loop[:self.index_of_segment(self.clm_loop, 'SVC')])],
            #Claim level service adjustments CAS
            'service_adjustments': functools.reduce(lambda x,y: x+y,
                [self.populate_adjustment_groups(x)
                 for x in self.segments_by_name("CAS",
                    data = self.clm_loop[1:min(  list(filter(lambda x: x>=0, [self.index_of_segment(self.clm_loop, 'SVC'), len(self.clm_loop)-1]))) ])], []),
            'claim_lines': [self.populate_claim_line(seg, i, min(self.index_of_segment(self.clm_loop, 'SVC', i+1), len(self.clm_loop)-1)) for i,seg in self.segments_by_name_index(segment_name="SVC", data=self.clm_loop)],
            'date_references': [{'date_cd': x.element(1), 'date': x.element(2)} for x in self.clm_loop if x.segment_name() == "DTM"]
        }

    def _populate_names(self, loop):
        return [
            {
                "entity_id_cd": x.element(1),
                "entity_type_qualifier": x.element(2),
                "entity_last_or_organization_name": x.element(3),
                "entity_first": x.element(4),
                "id_cd_qualifier": x.element(8),
                "id_cd": x.element(9)
            }
                 for x in loop if x.segment_name()== "NM1"]

    
    #
    # @parma svc - the svc segment for the service rendered
    # @param idx - the index where the svc is found within self.clm_loop
    # @param svc_end_idx - the last segment associated with the service 
    #
    def populate_claim_line(self, svc, idx, svc_end_idx):
        return {
            'prcdr_cd':svc.element(1),
            'chrg_amt':svc.element(2),
            'paid_amt':svc.element(3),
            'rev_cd':svc.element(4),
            'units': svc.element(5),
            'original_prcdr_cd':svc.element(6),
            'service_date_qualifier_cd': self._first(self.clm_loop, "DTM", idx).element(1),
            'service_date': self._first(self.clm_loop, "DTM", idx).element(2),
            'amt_qualifier_cd': self._first(self.clm_loop, "AMT", idx).element(1),
            'service_line_amt': self._first(self.clm_loop, "AMT", idx).element(2),
            'remarks': [{'qualifier_cd': x.element(1), 'remark_cd': x.element(2)} for x in self.segments_by_name("LQ", data = self.clm_loop[idx:svc_end_idx])],
            #line level service adjustments
            'service_adjustments': functools.reduce(lambda x,y: x+y,
                [self.populate_adjustment_groups(x) for x in self.segments_by_name("CAS", data =  self.clm_loop[idx:svc_end_idx])], []),
            'line_refs': [{'id_code_qualifier': x.element(1), 'id': x.element(2)} for x in  self.segments_by_name("REF", data=self.clm_loop[idx:svc_end_idx])]
        }

    #
    # group adjustment logic
    #
    def populate_adjustment_groups(self, cas):
        return [{'adjustment_grp_cd': (cas.element(1) if cas.element(i) == "" else cas.element(i)), 'adjustment_reason_cd': cas.element(i+1), 'adjustment_amount': cas.element(i+2)} for i in list(range(1, cas.segment_len()-1, 3))]

    def to_json(self):
        return {
            **{'payment': self.trx_header_info},
            **{'payer': self.payer_info},
            **{'payee': self.payee_info},
            **{'claim': self.clm_info},
            **{'provider_adjustments': self.plb_info}
        }
    
    
