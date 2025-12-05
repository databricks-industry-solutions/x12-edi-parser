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
        claim_loop = self.get_claim_loop(idx)
        sl_loop = self.get_service_line_loop(idx)
        
        # Provider section: segments between claim_loop end and service_line_loop start
        # This contains providers that come after claim-level segments but before service lines
        claim_loop_end_idx = idx + len(claim_loop)
        # Find first LX after claim
        lx_after_claim = [i for i, seg in enumerate(self.data[claim_loop_end_idx:], claim_loop_end_idx) if seg._name == "LX"]
        if lx_after_claim:
            provider_section = self.data[claim_loop_end_idx:lx_after_claim[0]]
        else:
            provider_section = []
        
        # Diagnosis section: Extract only HI segments from claim_loop
        # Structural rule: Diagnosis loop starts at first HI, ends at next LX, CLM, provider NM1, or HL
        diagnosis_section = self.get_diagnosis_loop(idx, claim_loop)
        
        return self.trnx_cls(
            sender_receiver_loop=self.get_submitter_receiver_loop(idx),
            billing_loop=self.loop.get_loop_segments(idx, "2000A"),
            subscriber_loop=self.loop.get_loop_segments(idx, "2000B"),
            patient_loop=self.loop.get_loop_segments(idx, "2000C"),
            claim_loop=claim_loop,
            sl_loop=sl_loop,  # service line loop
            provider_section=provider_section,  # segments between claim and service lines
            diagnosis_section=diagnosis_section,  # HI segments only
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
        return self.trnx_cls(
            enrollment_member = self.data[self.index_of_segment(self.data, "INS"): self.index_of_segment(self.data, "SE")],
            health_plan_loop=self.data[self.index_of_segment(self.data, "HD"): self.last_index_of_segment(self.data, "DTP")+1]
        )
    #
    # HL-hierarchy structural boundary helpers - use HL hierarchy as primary structural marker
    #
    def _find_next_hl(self, start_idx, required_level=None):
        """
        Find next HL segment after start_idx.
        If required_level is specified, finds HL with that hl_code (element 3).
        Returns (index, hl_segment) or (-1, None) if not found.
        """
        for i in range(start_idx + 1, len(self.data)):
            seg = self.data[i]
            if seg._name == 'HL':
                if required_level is None or seg.element(3) == required_level:
                    return (i, seg)
        return (-1, None)
    
    def _find_next_lx(self, start_idx):
        """Find next LX segment after start_idx. Returns index or -1 if not found."""
        for i in range(start_idx + 1, len(self.data)):
            if self.data[i]._name == 'LX':
                return i
        return -1
    
    def _get_hl_at_position(self, pos_idx):
        """Get HL segment information at position using loop hierarchy."""
        # Find HL loop that contains this position
        for hl_id, hl_info in self.loop.loop_hierarchy.items():
            if hl_info['start_idx'] <= pos_idx < hl_info['end_idx']:
                return hl_info
        return None
    
    def _is_hl_child_of(self, child_pos, parent_pos):
        """Check if HL at child_pos is a child of HL at parent_pos using hierarchy."""
        child_hl = self._get_hl_at_position(child_pos)
        parent_hl = self._get_hl_at_position(parent_pos)
        if child_hl and parent_hl:
            return child_hl.get('parent_id') == parent_hl.get('start_idx') or \
                   (child_hl.get('parent_id') != "" and 
                    self.loop.loop_hierarchy.get(str(child_hl.get('parent_id'))) == parent_hl)
        return False
    
    def _find_next_nm1_of_different_entity(self, start_idx, current_entity_code, segments=None):
        """
        Find next NM1 with different entity code, or next HL/LX that indicates structural boundary.
        Uses HL hierarchy to determine if NM1 is in different structural context.
        """
        if segments is None:
            segments = self.data
        
        start_hl = self._get_hl_at_position(start_idx)
        
        for i in range(start_idx + 1, len(segments)):
            seg = segments[i]
            
            # HL or LX always indicates structural boundary
            if seg._name == 'HL' or seg._name == 'LX':
                return i
            
            # Check NM1 entity code
            if seg._name == 'NM1':
                entity_code = seg.element(1)
                
                # Different entity code indicates different loop
                if entity_code != current_entity_code:
                    # Check if this NM1 is in different HL context
                    nm1_hl = self._get_hl_at_position(i)
                    if start_hl and nm1_hl:
                        # If different parent or different HL level, it's a boundary
                        if nm1_hl.get('parent_id') != start_hl.get('parent_id') or \
                           nm1_hl.get('hl_code') != start_hl.get('hl_code'):
                            return i
                    else:
                        # No HL context, different entity code is boundary
                        return i
        
        return len(segments)
    
    #
    # Determine claim loop: starts at CLM, stops at first HL with service-line child, next LX, or end
    # Structural rule: Use HL hierarchy to determine claim boundaries
    #
    def get_claim_loop(self, clm_idx):
        """
        Claim loop boundaries using HL hierarchy:
        - Start at CLM
        - Stop at first of:
          - Next HL that indicates new structural level (different parent or service-line child)
          - Next LX (service line start)
          - End of transaction
        """
        clm_hl = self._get_hl_at_position(clm_idx)
        
        # Find next LX (service line start)
        next_lx_idx = self._find_next_lx(clm_idx)
        
        # Find next HL and check if it's a structural boundary
        next_hl_idx, next_hl_seg = self._find_next_hl(clm_idx)
        
        # Check if next HL is a structural boundary (different parent or service-line indicator)
        hl_is_boundary = False
        if next_hl_idx != -1 and clm_hl:
            # If HL has different parent or is at different level, it's a boundary
            next_hl_info = self._get_hl_at_position(next_hl_idx)
            if next_hl_info:
                # Different parent indicates new structural section
                if next_hl_info.get('parent_id') != clm_hl.get('parent_id'):
                    hl_is_boundary = True
                # Service-line child indicator (HL code 19/30/31 with child_code=1)
                elif next_hl_info.get('child_code') == '1' and next_hl_info.get('hl_code') in ['19', '30', '31']:
                    hl_is_boundary = True
        
        # Find next CLM (next claim)
        next_clm_idx = -1
        for i in range(clm_idx + 1, len(self.data)):
            if self.data[i]._name == 'CLM':
                next_clm_idx = i
                break
        
        # Stop at earliest structural boundary
        candidates = []
        if next_lx_idx != -1:
            candidates.append(next_lx_idx)
        if next_hl_idx != -1 and hl_is_boundary:
            candidates.append(next_hl_idx)
        if next_clm_idx != -1:
            candidates.append(next_clm_idx)
        
        if candidates:
            clm_end_idx = min(candidates)
        else:
            clm_end_idx = len(self.data)
        
        return self.data[clm_idx:clm_end_idx]

    #
    # Service line loop: starts at LX, stops at next LX, next HL, or end of transaction
    # Structural rule: Use HL hierarchy - service lines are under claim HL, stop at next HL boundary
    #
    def get_service_line_loop(self, clm_idx):
        """
        Service line loop boundaries using HL hierarchy:
        - Start at first LX after claim
        - Stop at first of:
          - Next LX (next service line)
          - Next HL (structural boundary - new section)
          - End of transaction
        """
        # Find first LX after claim
        sl_start = self._find_next_lx(clm_idx)
        if sl_start == -1:
            return []
        
        sl_start_hl = self._get_hl_at_position(sl_start)
        
        # Find structural boundaries
        next_lx_idx = self._find_next_lx(sl_start)
        
        # Find next HL and check if it's a structural boundary
        next_hl_idx, next_hl_seg = self._find_next_hl(sl_start)
        hl_is_boundary = False
        if next_hl_idx != -1:
            # HL always indicates structural boundary for service lines
            hl_is_boundary = True
        
        # Find next CLM (next claim)
        next_clm_idx = -1
        for i in range(sl_start + 1, len(self.data)):
            if self.data[i]._name == 'CLM':
                next_clm_idx = i
                break
        
        # Find SE (transaction end)
        next_se_idx = -1
        for i in range(sl_start + 1, len(self.data)):
            if self.data[i]._name == 'SE':
                next_se_idx = i
                break
        
        # Stop at earliest structural boundary
        candidates = []
        if next_lx_idx != -1:
            candidates.append(next_lx_idx)
        if next_hl_idx != -1 and hl_is_boundary:
            candidates.append(next_hl_idx)
        if next_clm_idx != -1:
            candidates.append(next_clm_idx)
        if next_se_idx != -1:
            candidates.append(next_se_idx)
        
        if candidates:
            sl_end = min(candidates)
        else:
            sl_end = len(self.data)
        
        # Filter out HL segments (they mark hierarchy boundaries, not service line content)
        sl_segments = self.data[sl_start:sl_end]
        return [seg for seg in sl_segments if seg._name != 'HL']

    def get_diagnosis_loop(self, clm_idx, claim_loop):
        """
        Extract diagnosis loop: starts at first HI, ends before first 2310 provider NM1 or structural boundary.
        Structural rule: Diagnosis loop contains ONLY HI, DTP (claim-level), NTE (claim-level).
        Uses HL hierarchy to stop before 2310-level provider loops.
        """
        # Find first HI segment in claim_loop
        hi_start_idx = -1
        for i, seg in enumerate(claim_loop):
            if seg._name == "HI":
                hi_start_idx = i
                break
        
        if hi_start_idx == -1:
            return []
        
        # Get HL context for diagnosis
        claim_start_idx = clm_idx
        hi_absolute_idx = claim_start_idx + hi_start_idx
        diag_hl = self._get_hl_at_position(hi_absolute_idx)
        
        # Find structural boundaries: next LX, CLM, HL, or first provider NM1 under 2310 context
        next_lx_idx = self._find_next_lx(hi_absolute_idx)
        
        next_hl_idx, next_hl_seg = self._find_next_hl(hi_absolute_idx)
        hl_is_boundary = False
        if next_hl_idx != -1:
            # HL indicates structural boundary
            hl_is_boundary = True
        
        # Find next CLM
        next_clm_idx = -1
        for i in range(hi_absolute_idx + 1, len(self.data)):
            if self.data[i]._name == 'CLM':
                next_clm_idx = i
                break
        
        # Find first provider NM1 that indicates 2310-level boundary
        # Stop before provider loops (2310) - these are structurally after diagnosis
        next_provider_nm1_idx = -1
        for i in range(hi_absolute_idx + 1, len(self.data)):
            seg = self.data[i]
            if seg._name == 'NM1':
                # Check if this NM1 is in a different HL context (2310 provider level)
                nm1_hl = self._get_hl_at_position(i)
                if nm1_hl and diag_hl:
                    # If NM1 is in different HL context or indicates provider level, it's a boundary
                    if nm1_hl.get('parent_id') != diag_hl.get('parent_id') or \
                       nm1_hl.get('hl_code') != diag_hl.get('hl_code'):
                        next_provider_nm1_idx = i
                        break
                elif not diag_hl:
                    # No HL context, any NM1 after HI could be provider boundary
                    next_provider_nm1_idx = i
                    break
        
        # Stop at earliest boundary
        candidates = [idx for idx in [next_lx_idx, next_clm_idx, next_hl_idx, next_provider_nm1_idx] if idx != -1]
        if candidates:
            diag_end_idx = min(candidates)
        else:
            diag_end_idx = len(self.data)
        
        # Extract segments from first HI to end boundary
        # Include HI, DTP (claim-level), NTE (claim-level) - filter out others
        diag_segments = self.data[hi_absolute_idx:diag_end_idx]
        # Only include HI, DTP, NTE (diagnosis and claim-level supporting segments)
        filtered_segments = [seg for seg in diag_segments if seg._name in ['HI', 'DTP', 'NTE']]
        
        return filtered_segments
    
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
        provider_section: List = [],  # Segments between claim_loop and sl_loop
        diagnosis_section: List = [],  # HI segments only
    ):
        self.sender_receiver_loop = sender_receiver_loop # extracted together
        self.billing_loop = billing_loop
        self.subscriber_loop = subscriber_loop
        self.patient_loop = patient_loop
        self.claim_loop = claim_loop
        self.sl_loop = sl_loop
        self.provider_section = provider_section  # Contains providers between claim and service lines
        self.diagnosis_section = diagnosis_section  # Contains only HI segments
        self.build()

    #
    # Structural boundary helpers - use only HL, NM1, LX for boundaries
    #
    def _find_next_nm1_of_different_entity(self, start_idx, current_entity_code, segments=None):
        """
        Find next NM1 with different entity code, or next HL/LX that indicates structural boundary.
        Works with both absolute indices (self.data) and relative indices (segments list).
        """
        if segments is None:
            segments = self.data
            use_absolute_idx = True
        else:
            use_absolute_idx = False
        
        # Get HL context if using absolute indices (requires access to ClaimBuilder's loop)
        # For relative indices, just use entity code comparison
        for i in range(start_idx + 1, len(segments)):
            seg = segments[i]
            
            # HL or LX always indicates structural boundary
            if seg._name == 'HL' or seg._name == 'LX':
                return i
            
            # Check NM1 entity code
            if seg._name == 'NM1':
                entity_code = seg.element(1)
                
                # Different entity code indicates different loop
                if entity_code != current_entity_code:
                    return i
        
        return len(segments)
    
    def _find_next_provider_nm1_or_hl_or_lx(self, start_idx, segments):
        """Find next provider NM1 (entity codes: 71,72,73,77,82,DN,PE,RF,AT,OP,OT) or HL or LX."""
        provider_codes = {'71', '72', '73', '77', '82', 'DN', 'PE', 'RF', 'AT', 'OP', 'OT'}
        for i in range(start_idx + 1, len(segments)):
            seg = segments[i]
            if seg._name == 'HL' or seg._name == 'LX':
                return i
            if seg._name == 'NM1' and seg.element(1) in provider_codes:
                return i
        return len(segments)

    #
    # Return first segment found of name == name otherwise Segment.empty()
    #
    def _first(self, segments, name, start_index = 0):
        return ([x for x in segments[start_index:] if x._name == name][0]  if len([x for x in segments[start_index:] if x._name == name]) > 0 else Segment.empty())
        
    def _populate_providers(self):
        return {"billing": self._billing_provider()}
    
    def _billing_provider(self):
        return ProviderIdentity(segments=self.billing_loop)

    def _populate_diagnosis(self):
        # Structural rule: Diagnosis loop contains ONLY HI segments
        # Use diagnosis_section which contains only HI segments, not claim_loop which has CLM/DTP/REF/NTE
        diagnosis_segments = self.diagnosis_section if hasattr(self, 'diagnosis_section') else []
        return DiagnosisIdentity(segments=diagnosis_segments)
    
    def _populate_submitter_loop(self) -> Dict[str, str]:
        # Find the submitter NM1 (entity code 41)
        submitter_idx = -1
        for i, seg in enumerate(self.sender_receiver_loop):
            if seg._name == "NM1" and seg.element(1) == "41":
                submitter_idx = i
                break
        
        if submitter_idx == -1:
            return Submitter_Receiver_Identity(segments=[])
        
        # Slice until the next NM1 or end of loop
        next_nm1_idx = -1
        for i in range(submitter_idx + 1, len(self.sender_receiver_loop)):
            if self.sender_receiver_loop[i]._name == "NM1":
                next_nm1_idx = i
                break
        
        end_idx = next_nm1_idx if next_nm1_idx != -1 else len(self.sender_receiver_loop)
        # GREEDY: Pass everything (NM1, PER, REF, etc.)
        return Submitter_Receiver_Identity(segments=self.sender_receiver_loop[submitter_idx:end_idx])
    
    def _populate_receiver_loop(self) -> Dict[str, str]:
        # Find the receiver NM1 (entity code 40)
        receiver_idx = -1
        for i, seg in enumerate(self.sender_receiver_loop):
            if seg._name == "NM1" and seg.element(1) == "40":
                receiver_idx = i
                break
        
        if receiver_idx == -1:
            return Submitter_Receiver_Identity(segments=[])
        
        # Slice until the next NM1 or end of loop
        next_nm1_idx = -1
        for i in range(receiver_idx + 1, len(self.sender_receiver_loop)):
            if self.sender_receiver_loop[i]._name == "NM1":
                next_nm1_idx = i
                break
        
        end_idx = next_nm1_idx if next_nm1_idx != -1 else len(self.sender_receiver_loop)
        # GREEDY: Pass everything (NM1, PER, REF, etc.)
        return Submitter_Receiver_Identity(segments=self.sender_receiver_loop[receiver_idx:end_idx])

    def _populate_subscriber_loop(self):
        """
        Extract subscriber loop: starts at HL*22 or SBR, ends at next NM1 (PR/DN/82/77), CLM, HL, or LX.
        Structural rule: Subscriber loop contains SBR, NM1*IL, N3, N4, DMG, REF (if present).
        """
        if not self.subscriber_loop:
            return PatientIdentity(segments=[])
        
        # Find start: first SBR or HL*22 (subscriber loop marker)
        start_idx = 0
        for i, seg in enumerate(self.subscriber_loop):
            if seg._name == "SBR" or (seg._name == "HL" and seg.element(3) == "22"):
                start_idx = i
                break
        
        # Find structural boundaries: next NM1 (PR/DN/82/77), CLM, HL, or LX
        # These mark the end of the subscriber section
        provider_codes = {'PR', 'DN', '82', '77', '71', '72', '73'}
        next_provider_nm1_idx = -1
        next_clm_idx = -1
        next_hl_idx = -1
        next_lx_idx = -1
        
        for i in range(start_idx + 1, len(self.subscriber_loop)):
            seg = self.subscriber_loop[i]
            if seg._name == "NM1" and seg.element(1) in provider_codes and next_provider_nm1_idx == -1:
                next_provider_nm1_idx = i
            elif seg._name == "CLM" and next_clm_idx == -1:
                next_clm_idx = i
            elif seg._name == "HL" and next_hl_idx == -1:
                # Skip the HL that starts this subscriber loop
                if i > start_idx:
                    next_hl_idx = i
            elif seg._name == "LX" and next_lx_idx == -1:
                next_lx_idx = i
        
        # Stop at earliest boundary
        candidates = [idx for idx in [next_provider_nm1_idx, next_clm_idx, next_hl_idx, next_lx_idx] if idx != -1]
        end_idx = min(candidates) if candidates else len(self.subscriber_loop)
        
        # Extract subscriber segments (SBR, NM1*IL, N3, N4, DMG, REF, etc.)
        subscriber_segments = self.subscriber_loop[start_idx:end_idx]
        return PatientIdentity(segments=subscriber_segments)
    
    def _populate_patient_loop(self) -> Dict[str, str]:
        """
        Extract patient loop: 
        - If relationship code is "18" (Self), patient is same as subscriber
        - Otherwise, look for NM1*QC (patient loop) or use patient_loop if provided
        """
        if not self.subscriber_loop:
            return PatientIdentity(segments=[])
        
        # Check SBR relationship code: 18 = Self, so patient = subscriber
        sbr_seg = self._first(self.subscriber_loop, "SBR")
        if sbr_seg and sbr_seg.element(2) == "18":
            # Patient is same as subscriber
            return self._populate_subscriber_loop()
        
        # Patient is different - look for NM1*QC in patient_loop
        if self.patient_loop:
            # Find start: first NM1*QC or HL*23 (patient loop marker)
            start_idx = 0
            for i, seg in enumerate(self.patient_loop):
                if seg._name == "NM1" and seg.element(1) == "QC":
                    start_idx = i
                    break
                elif seg._name == "HL" and seg.element(3) == "23":
                    start_idx = i
                    break
            
            # Find structural boundaries: next NM1 (PR/DN/82/77), CLM, HL, or LX
            provider_codes = {'PR', 'DN', '82', '77', '71', '72', '73'}
            next_provider_nm1_idx = -1
            next_clm_idx = -1
            next_hl_idx = -1
            next_lx_idx = -1
            
            for i in range(start_idx + 1, len(self.patient_loop)):
                seg = self.patient_loop[i]
                if seg._name == "NM1" and seg.element(1) in provider_codes and next_provider_nm1_idx == -1:
                    next_provider_nm1_idx = i
                elif seg._name == "CLM" and next_clm_idx == -1:
                    next_clm_idx = i
                elif seg._name == "HL" and next_hl_idx == -1:
                    if i > start_idx:
                        next_hl_idx = i
                elif seg._name == "LX" and next_lx_idx == -1:
                    next_lx_idx = i
            
            # Stop at earliest boundary
            candidates = [idx for idx in [next_provider_nm1_idx, next_clm_idx, next_hl_idx, next_lx_idx] if idx != -1]
            end_idx = min(candidates) if candidates else len(self.patient_loop)
            
            # Extract patient segments
            patient_segments = self.patient_loop[start_idx:end_idx]
            return PatientIdentity(segments=patient_segments)
        
        # No separate patient loop found, return empty
        return PatientIdentity(segments=[])
    
    def _populate_claim_loop(self):
        return ClaimIdentity(segments=self.claim_loop)


                             

    def _populate_payer_info(self):
        """
        Extract payer loop: starts at NM1*PR, stops at next NM1 (unless same 2010BB), next HL, or next CLM.
        Structural rule: Use HL hierarchy - payer is under 2010BB, stop at structural boundaries.
        """
        if not self.subscriber_loop:
            return PayerIdentity(segments=[])
        
        # Find the Payer NM1 (entity code PR)
        payer_idx = -1
        for i, seg in enumerate(self.subscriber_loop):
            if seg._name == "NM1" and seg.element(1) == "PR":
                payer_idx = i
                break
        
        if payer_idx == -1:
            return PayerIdentity(segments=[])
        
        # Structural rule: Payer loop ends at next NM1 (unless same 2010BB sequence), HL, or CLM
        # Use structural helper to find next different entity or structural marker
        end_idx = self._find_next_nm1_of_different_entity(payer_idx, "PR", self.subscriber_loop)
        
        # Also check for CLM boundary
        clm_idx = -1
        for i in range(payer_idx + 1, len(self.subscriber_loop)):
            if self.subscriber_loop[i]._name == "CLM":
                clm_idx = i
                break
        
        if clm_idx != -1 and clm_idx < end_idx:
            end_idx = clm_idx
        
        # GREEDY: Pass everything (N3, N4, REF, PER, etc.)
        return PayerIdentity(segments=self.subscriber_loop[payer_idx:end_idx])
    
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
        """
        Extract service facility provider: starts at NM1*77, stops at next NM1 (different entity), HL, or LX.
        Structural rule: Use HL hierarchy - provider loops are under 2310, stop at structural boundaries.
        """
        # Search in provider_section (between claim_loop and sl_loop)
        search_segments = self.provider_section if hasattr(self, 'provider_section') else self.claim_loop
        
        if not search_segments:
            return ProviderIdentity(segments=[])
        
        # Find index of service facility NM1 (entity code 77)
        facility_idx = -1
        for i, seg in enumerate(search_segments):
            if seg._name == "NM1" and seg.element(1) == "77":
                facility_idx = i
                break
        
        if facility_idx == -1:
            return ProviderIdentity(segments=[])
        
        # Use structural boundary helper to find next different entity or structural marker
        end_idx = self._find_next_nm1_of_different_entity(facility_idx, "77", search_segments)
        # GREEDY: Pass everything (NM1, N3, N4, REF, PRV, etc.)
        return ProviderIdentity(segments=search_segments[facility_idx:end_idx])

    #
    # Returns each claim line as an array of segments that make up the claim line
    # Structural rule: Each service line starts at LX and ends at next LX or end
    # IMPORTANT: HL segments must NOT appear in service lines (they mark hierarchy boundaries)
    #
    def claim_lines(self):
        lx_indices = [i for i, seg in enumerate(self.sl_loop) if seg._name == "LX"]
        if not lx_indices:
            return []
        
        service_lines = []
        for i, lx_idx in enumerate(lx_indices):
            # Start at this LX
            start_idx = lx_idx
            
            # End at next LX or end of sl_loop
            if i + 1 < len(lx_indices):
                end_idx = lx_indices[i + 1]
            else:
                end_idx = len(self.sl_loop)
            
            # Extract service line segments and filter out HL (structural boundary marker)
            line_segments = self.sl_loop[start_idx:end_idx]
            # Remove HL segments - they mark hierarchy boundaries, not service line content
            filtered_segments = [seg for seg in line_segments if seg._name != 'HL']
            service_lines.append(filtered_segments)
        
        return service_lines

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
        """
        Extract attending provider: starts at NM1*71, stops at next NM1 (different entity), HL, or LX.
        Structural rule: Use HL hierarchy - provider loops are under 2310, stop at structural boundaries.
        """
        search_segments = self.provider_section if hasattr(self, 'provider_section') else self.claim_loop
        
        if not search_segments:
            return ProviderIdentity(segments=[])
        
        attending_idx = -1
        for i, seg in enumerate(search_segments):
            if seg._name == "NM1" and seg.element(1) == "71":
                attending_idx = i
                break
        
        if attending_idx == -1:
            return ProviderIdentity(segments=[])
        
        end_idx = self._find_next_nm1_of_different_entity(attending_idx, "71", search_segments)
        return ProviderIdentity(segments=search_segments[attending_idx:end_idx])

    def _operating_provider(self):
        """
        Extract operating provider: starts at NM1*72, stops at next NM1 (different entity), HL, or LX.
        Structural rule: Use HL hierarchy - provider loops are under 2310, stop at structural boundaries.
        """
        search_segments = self.provider_section if hasattr(self, 'provider_section') else self.claim_loop
        
        if not search_segments:
            return ProviderIdentity(segments=[])
        
        operating_idx = -1
        for i, seg in enumerate(search_segments):
            if seg._name == "NM1" and seg.element(1) == "72":
                operating_idx = i
                break
        
        if operating_idx == -1:
            return ProviderIdentity(segments=[])
        
        end_idx = self._find_next_nm1_of_different_entity(operating_idx, "72", search_segments)
        return ProviderIdentity(segments=search_segments[operating_idx:end_idx])

    def _other_provider(self):
        """
        Extract other provider: starts at NM1*73, stops at next NM1 (different entity), HL, or LX.
        Structural rule: Use HL hierarchy - provider loops are under 2310, stop at structural boundaries.
        """
        search_segments = self.provider_section if hasattr(self, 'provider_section') else self.claim_loop
        
        if not search_segments:
            return ProviderIdentity(segments=[])
        
        other_idx = -1
        for i, seg in enumerate(search_segments):
            if seg._name == "NM1" and seg.element(1) == "73":
                other_idx = i
                break
        
        if other_idx == -1:
            return ProviderIdentity(segments=[])
        
        end_idx = self._find_next_nm1_of_different_entity(other_idx, "73", search_segments)
        return ProviderIdentity(segments=search_segments[other_idx:end_idx]) 
      
    def _populate_providers(self):
        return {"billing": self._billing_provider(),
                "attending": self._attending_provider(),
                "operating": self._operating_provider(),
                "other": self._other_provider(),
                "service_facility": self._service_facility_provider()
                }

    def _populate_claim_loop(self):
        return ClaimIdentity(segments=self.claim_loop)

    def _populate_sl_loop(self, missing=""):
        return list(
            map(lambda s:
                ServiceLine.from_sv2(
                    segments=s,  # <--- NEW: Pass full raw segments
                    sv2 = self._first(s, "SV2"),
                    lx = self._first(s, "LX"),
                    dtp = self.segments_by_name("DTP", data = s),
                    amt = self.segments_by_name("AMT", data=s)
                ),self.claim_lines()))

    
class Claim837p(MedicalClaim):

    NAME = "837P"
    # Format of 837P https://www.dhs.wisconsin.gov/publications/p0/p00265.pdf

    def _rendering_provider(self):
        """
        Extract rendering provider: starts at NM1*82, stops at next NM1 (different entity), HL, or LX.
        Structural rule: Use HL hierarchy - provider loops are under 2310, stop at structural boundaries.
        """
        search_segments = self.provider_section if hasattr(self, 'provider_section') else self.claim_loop
        
        if not search_segments:
            return ProviderIdentity(segments=[])
        
        rendering_idx = -1
        for i, seg in enumerate(search_segments):
            if seg._name == "NM1" and seg.element(1) == "82":
                rendering_idx = i
                break
        
        if rendering_idx == -1:
            return ProviderIdentity(segments=[])
        
        end_idx = self._find_next_nm1_of_different_entity(rendering_idx, "82", search_segments)
        return ProviderIdentity(segments=search_segments[rendering_idx:end_idx])
    
    def _referring_provider(self):
        """
        Extract referring provider: starts at NM1*DN, stops at next NM1 (different entity), HL, or LX.
        Structural rule: Use HL hierarchy - provider loops are under 2310, stop at structural boundaries.
        """
        search_segments = self.provider_section if hasattr(self, 'provider_section') else self.claim_loop
        
        if not search_segments:
            return ProviderIdentity(segments=[])
        
        referring_idx = -1
        for i, seg in enumerate(search_segments):
            if seg._name == "NM1" and seg.element(1) == "DN":
                referring_idx = i
                break
        
        if referring_idx == -1:
            return ProviderIdentity(segments=[])
        
        end_idx = self._find_next_nm1_of_different_entity(referring_idx, "DN", search_segments)
        return ProviderIdentity(segments=search_segments[referring_idx:end_idx])


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
                    segments=s,  # <--- NEW: Pass full raw segments
                    sv1 = self._first(s, "SV1"),
                    lx = self._first(s, "LX"),
                    dtp = self.segments_by_name("DTP", data=s),
                    amt = self.segments_by_name("AMT", data=s)
                ), self.claim_lines()))
