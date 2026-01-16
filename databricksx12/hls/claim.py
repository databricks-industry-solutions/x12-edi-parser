from databricksx12.edi import EDI, AnsiX12Delim, Segment
from databricksx12.hls.identities import *
from collections import defaultdict
import json
import bisect


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

    #
    # Builds a claim object from
    #
    # @param clm_segment - the claim segment of claim to build
    # @param idx - the index of the claim segment in the data
    #
    #  @return the class containing the relevent claim information
    #
    def build_claim(self, clm_segment, idx, billing_loop, subscriber_loop, patient_loop):
        return self.trnx_cls(
            sender_receiver_loop=self.get_submitter_receiver_loop(idx),
            billing_loop=billing_loop,
            subscriber_loop=subscriber_loop,
            patient_loop=patient_loop,
            claim_loop=self.get_claim_loop(idx),
            sl_loop=self.get_service_line_loop(idx),  # service line loop
        )


        
    #
    # https://datainsight.health/edi/payments/dollars-separate/
    #  trx_header_loop = 0000
    #  payer_loop = 1000A
    #  payee_loop = 1000B
    #  header_number_loop = 2000
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
                             ,header_number_loop = self.data[self.index_of_segment(self.data, "LX"):idx]
                            )

    def build_enrollment(self, pay_segment, idx):
        ins_next_idx = self.index_of_segment(self.data, "INS", idx+1)
        end_idx = ins_next_idx if ins_next_idx > 0 else self.index_of_segment(self.data, "SE")
        
        return self.trnx_cls(
            member_detail_loop = self.data[idx:end_idx]
        )

    #
    # Need to end the subscriber list at the start of the clm
    #
    def get_subscriber_loop(self, subscriber_loop_plus): 
        return subscriber_loop_plus[0:(self.index_of_segment(subscriber_loop_plus, "CLM") if self.index_of_segment(subscriber_loop_plus, "CLM") > 0 else len(subscriber_loop_plus)-1)]
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
        sl_starts = list(map(lambda x: x[0], filter(lambda x: x[0] > clm_idx, self.segments_by_name_index("LX"))))
        if not sl_starts:
            return []
        sl_start = min(sl_starts)
        clm_idxs = list(map(lambda x: x[0],filter(lambda x: x[0] > clm_idx, self.segments_by_name_index("CLM"))))
        se_idxs = list(map(lambda x: x[0], filter(lambda x: x[0] > clm_idx, self.segments_by_name_index("SE"))))
        sl_end = min(clm_idxs + se_idxs + [len(self.data)])
        return self.data[sl_start:sl_end]

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
    def _build_837_iter(self):
        """Generator for 837 claims - true single pass with delayed yield."""
        # HL loop tracking (immutable tuples: start, end)
        billing_loop = (-1, -1)
        subscriber_loop = (-1, -1)
        patient_loop = (-1, -1)
        
        # Sender/receiver tracking (computed once)
        bht_idx = -1
        first_hl20_idx = -1
        sender_receiver_loop = None  # Computed lazily
        
        # Pending claim state (immutable tuple):
        # (clm_idx, billing_loop, subscriber_loop, patient_loop, claim_end, sl_start)
        pending = None
        
        for i, seg in enumerate(self.data):
            name = seg._name
            
            if name == "BHT":
                if bht_idx == -1:
                    bht_idx = i
                    
            elif name == "HL":
                code = seg.element(3)
                if code == "20":
                    if first_hl20_idx == -1:
                        first_hl20_idx = i
                        sender_receiver_loop = self.data[bht_idx:i] if bht_idx != -1 else []
                    # Close billing at this HL*20
                    if billing_loop[0] != -1 and billing_loop[1] == -1:
                        billing_loop = (billing_loop[0], i)
                    billing_loop = (i, -1)
                    subscriber_loop = (-1, -1)
                    patient_loop = (-1, -1)
                elif code == "22":
                    if billing_loop[0] != -1 and billing_loop[1] == -1:
                        billing_loop = (billing_loop[0], i)
                    subscriber_loop = (i, -1)
                    patient_loop = (-1, -1)
                elif code == "23":
                    if subscriber_loop[0] != -1 and subscriber_loop[1] == -1:
                        subscriber_loop = (subscriber_loop[0], i)
                    patient_loop = (i, -1)
                    
            elif name == "LX":
                # First LX after CLM marks claim_end and sl_start
                if pending is not None and pending[4] == -1:
                    pending = (pending[0], pending[1], pending[2], pending[3], i, i)
                    
            elif name == "CLM":
                # Yield previous pending claim (now we know sl_end = i)
                if pending is not None:
                    yield self._build_claim_from_pending(pending, sl_end=i, sender_receiver=sender_receiver_loop)
                
                # Snapshot current loops for new pending claim
                sub_end = subscriber_loop[1] if subscriber_loop[1] != -1 else i
                pat_end = patient_loop[1] if patient_loop[1] != -1 else i
                
                # pending = (clm_idx, billing, subscriber, patient, claim_end, sl_start)
                pending = (
                    i,
                    self.data[billing_loop[0]:billing_loop[1]] if billing_loop[0] != -1 else [],
                    self.data[subscriber_loop[0]:sub_end] if subscriber_loop[0] != -1 else [],
                    self.data[patient_loop[0]:pat_end] if patient_loop[0] != -1 else [],
                    -1,  # claim_end unknown
                    -1,  # sl_start unknown
                )
                
            elif name == "SE":
                # End of transaction - yield final pending claim
                if pending is not None:
                    # If no LX was seen, claim goes to SE
                    claim_end = pending[4] if pending[4] != -1 else i
                    sl_start = pending[5] if pending[5] != -1 else i
                    pending = (pending[0], pending[1], pending[2], pending[3], claim_end, sl_start)
                    yield self._build_claim_from_pending(pending, sl_end=i, sender_receiver=sender_receiver_loop)
                    pending = None
        
        # Handle case with no SE
        if pending is not None:
            end = len(self.data)
            claim_end = pending[4] if pending[4] != -1 else end
            sl_start = pending[5] if pending[5] != -1 else end
            pending = (pending[0], pending[1], pending[2], pending[3], claim_end, sl_start)
            yield self._build_claim_from_pending(pending, sl_end=end, sender_receiver=sender_receiver_loop)

    def _build_claim_from_pending(self, pending, sl_end, sender_receiver):
        """Build claim from pending tuple. No side effects."""
        clm_idx, billing, subscriber, patient, claim_end, sl_start = pending
        return self.trnx_cls(
            sender_receiver_loop=sender_receiver if sender_receiver else [],
            billing_loop=billing,
            subscriber_loop=subscriber,
            patient_loop=patient,
            claim_loop=self.data[clm_idx:claim_end],
            sl_loop=self.data[sl_start:sl_end] if sl_start < sl_end else [],
        )

    def build(self):
        if self.trnx_cls.NAME in ['837I', '837P']:
            return list(self._build_837_iter())
            
        elif self.trnx_cls.NAME == '835':
            # Optimized: Pre-build indices for all segment types needed
            clp_indices = [i for i, seg in self.segments_by_name_index("CLP")]
            n1_indices = [i for i, seg in self.segments_by_name_index("N1")]
            lx_indices = [i for i, seg in self.segments_by_name_index("LX")]
            svc_indices = [i for i, seg in self.segments_by_name_index("SVC")]
            se_indices = [i for i, seg in self.segments_by_name_index("SE")]
            
            # Pre-compute common values used by all remittances
            n1_first = n1_indices[0] if n1_indices else len(self.data)
            n1_second = n1_indices[1] if len(n1_indices) > 1 else len(self.data)
            lx_first = lx_indices[0] if lx_indices else len(self.data)
            lx_last = lx_indices[-1] if lx_indices else 0
            clp_last = clp_indices[-1] if clp_indices else 0
            svc_last = svc_indices[-1] if svc_indices else 0
            
            # Pre-build lookup for "next CLP after current index"
            clp_indices_set = set(clp_indices)
            
            remittances = []
            for idx_pos, idx in enumerate(clp_indices):
                # Find next CLP index
                next_clp = clp_indices[idx_pos + 1] if idx_pos + 1 < len(clp_indices) else -1
                
                # Find next LX after current idx
                next_lx = next((lx for lx in lx_indices if lx > idx), -1)
                
                # Find next SE after current idx
                next_se = next((se for se in se_indices if se > idx), -1)
                
                # Calculate clm_loop end
                clm_end = min(filter(lambda x: x > 0, [next_lx, next_clp, next_se, len(self.data)]))
                
                remittances.append(
                    self.trnx_cls(
                        trx_header_loop=self.data[0:n1_first],
                        payer_loop=self.data[n1_first:n1_second],
                        payee_loop=self.data[n1_second:lx_first],
                        clm_loop=self.data[idx:clm_end],
                        trx_summary_loop=self.data[max(0, lx_last, clp_last, svc_last):],
                        header_number_loop=self.data[lx_first:idx]
                    )
                )
            return remittances
            
        elif self.trnx_cls.NAME == '834':
            # Optimized: Pre-build index of all INS positions, then slice between them
            ins_indices = [i for i, seg in self.segments_by_name_index("INS")]
            se_idx = self.index_of_segment(self.data, "SE")
            
            # Add SE index as the end boundary
            ins_indices.append(se_idx if se_idx > 0 else len(self.data))
            
            # Build enrollments by slicing between consecutive INS positions
            return [
                self.trnx_cls(member_detail_loop=self.data[ins_indices[i]:ins_indices[i+1]])
                for i in range(len(ins_indices) - 1)
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
        # Optimized: Iterate once, check name, and return immediately
        for x in (segments[start_index:] if start_index > 0 else segments):
            if x._name == name:
                return x
        return Segment.empty()
        
    def _populate_providers(self):
        return {"billing": self._billing_provider()}
    
    def _billing_provider(self):
        return ProviderIdentity(nm1=self._first(self.billing_loop, "NM1"),
                                n3=self._first(self.billing_loop, "N3"),
                                n4=self._first(self.billing_loop, "N4"),
                                ref=self._first(self.billing_loop, "REF"))

    def _populate_diagnosis(self):
        return DiagnosisIdentity([x for x in self.claim_loop if x._name == "HI"])
    
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
                             ref = self.segments_by_name("REF", data=self.claim_loop[:(len(self.claim_loop)-1 if (temp := [i for i, x in enumerate(self.claim_loop) if x._name == "NM1"]) == [] else temp[0]) ]), #ref up until loop 2310
                             amt = self.segments_by_name("AMT", data=self.claim_loop)) #ref up until loop 2310

                             
    

    def _populate_payer_info(self):
        return PayerIdentity(self._first([x for x in self.subscriber_loop if x.element(1) == "PR"], "NM1"))
    
    def __str__(self):
        return json.dumps(self.to_json(), indent=4)

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
            **{'diagnosis': self.diagnosis_info.to_dict()},
            **{'input_loop_segments': self.loop_segments_info} #the input loops
        }

    def _service_facility_provider(self):
        i, nm1 = ((-1,Segment.empty()) if (temp := [(i,x) for i, x in enumerate(self.claim_loop) if x.element(1) == "77" and x._name == "NM1"]) == [] else temp[0])
        return ProviderIdentity(nm1=nm1,
                                n3=self._first([x for x in self.claim_loop[i+1:i+3]],"N3"),
                                n4=self._first([x for x in self.claim_loop[i+1:i+3]],"N4"),
                                ref= Segment.empty())

    #
    # Returns each claim line as an array of segments that make up the claim line
    #
    def claim_lines(self):
        return list(map(lambda i: self.sl_loop[i[0]:i[1]],
                self._index_to_tuples([(i) for i,y in enumerate(self.sl_loop) if y._name=="LX"]+[len(self.sl_loop)])))

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
        self.loop_segments_info =  {
            'sender_receiver_loop': self._extract_segments(self.sender_receiver_loop),
            'billing_loop': self._extract_segments(self.billing_loop),
            'subscriber_loop': self._extract_segments(self.subscriber_loop),
            'patient_loop': self._extract_segments(self.patient_loop ),
            'claim_loop': self._extract_segments(self.claim_loop ),
            'sl_loop': self._extract_segments(self.sl_loop)
        }

class Claim837i(MedicalClaim):

    NAME = "837I"

    # Format for 837I https://www.dhs.wisconsin.gov/publications/p0/p00266.pdf
    
    def _attending_provider(self):
        i, nm1 = ((-1,Segment.empty()) if (temp := [(i,x) for i, x in enumerate(self.claim_loop) if x.element(1) == "71" and x._name == "NM1"]) == [] else temp[0])
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
                             hi = self._first([x for x in self.claim_loop if x._name == "HI" and x.element(1).startswith("DR")], "HI"), #DRG_CD
                             ref = self.segments_by_name("REF", data=self.claim_loop[:(len(self.claim_loop)-1 if (temp := [i for i, x in enumerate(self.claim_loop) if x._name == "NM1"]) == [] else temp[0]) ]), #ref up until loop 2310 
                             amt = self.segments_by_name("AMT", data=self.claim_loop),
                             principal_hi = self._first([x for x in self.claim_loop if x._name == "HI" and x.element(1,0) == ("BBR")], "HI"),
                             other_hi = [x for x in self.claim_loop if x._name == "HI" and x.element(1,0) == ("BBQ")],
                             condition_hi = [x for x in self.claim_loop if x._name == "HI" and x.element(1,0) == "BG"]
                             )

    def _populate_sl_loop(self, missing=""):
        return list(
            map(lambda s:
                ServiceLine.from_sv2(
                    sv2 = self._first(s, "SV2"),
                    lx = self._first(s, "LX"),
                    dtp = self.segments_by_name("DTP", data = s),
                    amt = self.segments_by_name("AMT", data=s),
                    lin = self.segments_by_name("LIN", data=s)
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
                    amt = self.segments_by_name("AMT", data=s),
                    lin = self.segments_by_name("LIN", data=s)
                ), self.claim_lines()))
