from databricksx12.edi import EDI, AnsiX12Delim, Segment
from databricksx12.hls.loop import Loop
from databricksx12.hls.identities_enhanced import *
from typing import List, Dict
import functools
from collections import defaultdict

class MedicalClaim(EDI):
    def __init__(
        self,
        bht_loop: List = [],
        sender_receiver_loop: List = [],
        billing_loop: List = [],
        subscriber_loop: List = [],
        patient_loop: List = [],
        claim_loop: List = [],
        sl_loop: List = [],
        ref_loop: List = [], 
    ):
        self.bht_loop = bht_loop
        self.sender_receiver_loop = sender_receiver_loop
        self.billing_loop = billing_loop
        self.subscriber_loop = subscriber_loop
        self.patient_loop = patient_loop
        self.claim_loop = claim_loop
        self.sl_loop = sl_loop
        self.ref_loop = ref_loop
        self.build()

    def _first(self, segments, name, start_index = 0):
        data =  ([x for x in segments[start_index:] if x.segment_name() == name][0]  if len([x for x in segments[start_index:] if x.segment_name() == name]) > 0 else Segment.empty())
        return data

    def _populate_providers(self):
        return {"billing": self._billing_provider()}
    
    def _populate_bht(self):
        return BhtIdentity(bht=self._first(self.bht_loop, "BHT"))

    def _billing_provider(self):
        return ProviderIdentity(
                                hl=self._first(self.billing_loop, "HL"),
                                nm1=self._first(self.billing_loop, "NM1"),
                                n3=self._first(self.billing_loop, "N3"),
                                n4=self._first(self.billing_loop, "N4"),
                                ref=self._first(self.billing_loop, "REF"))
    
    def _pay_provider(self):
        """Populate the pay provider identity from the billing loop"""
        index_by_segment_name =  [index for index, segment in enumerate(self.billing_loop) if segment.segment_name() == "NM1" and segment.element(1) == "87"]
        if index_by_segment_name:
            self.pay_provider_loop = self.billing_loop[index_by_segment_name[0]:]
            return ProviderIdentity(
                                    nm1=self._first([x for x in self.pay_provider_loop if x.element(1) == "87"],"NM1"),
                                    n3=self._first(self.pay_provider_loop, "N3"),
                                    n4=self._first(self.pay_provider_loop, "N4"))

    def _populate_diagnosis(self):
        return DiagnosisIdentity([x for x in self.claim_loop if x.segment_name() == "HI"])
    
    def _populate_submitter_loop(self) -> Dict[str, str]:
        return Submitter_Receiver_Identity(nm1=self._first([x for x in self.sender_receiver_loop if x.element(1) == "41"],"NM1"),
                                           per=self._first(self.sender_receiver_loop, "PER"))
    
    def _populate_receiver_loop(self) -> Dict[str, str]:
        return Submitter_Receiver_Identity(nm1=self._first([x for x in self.sender_receiver_loop if x.element(1) == "40"],"NM1"))

    def _populate_subscriber_loop(self):
        l = self.subscriber_loop[0:min(filter(lambda x: x!= -1, [self.index_of_segment(self.subscriber_loop, "CLM"), len(self.subscriber_loop)]))]
        return PatientIdentity(
            hl=self._first(l, "HL"),
            nm1 = self._first(l, "NM1"),
            n3 = self._first(l, "N3"),
            n4 = self._first(l, "N4"),
            dmg = self._first(l, "DMG"),            
            pat = self._first(l, "PAT"),
            sbr = self._first(l, "SBR"),
            ref = self._first(l, "REF")
        )
    
    def _populate_patient_loop(self) -> Dict[str, str]:
        return self._populate_subscriber_loop() if self._first(self.subscriber_loop, "SBR").element(2) == "18" else PatientIdentity(
            # hl = self._first([x for x in self.patient_loop if x.element(3) == "23"], "HL"),
            hl = self._first(self.patient_loop, "HL"),
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
                             ref = self._first(self.claim_loop, "REF"))

    def _populate_ref_loop(self):
        return RefIdentity(ref = self._first(self.claim_loop, "REF"))
                             
    def _populate_payer_info(self):
        return PayerIdentity(self._first([x for x in self.subscriber_loop if x.element(1) == "PR"], "NM1"))
    
    def to_denormalized_json(self, filename='', transaction_control_number=''):
        """Convert claim to single-row denormalized structure"""
        
        # Base metadata
        base_data = {
            'filename': filename,
            'transaction_control_number': transaction_control_number,
            'claim_id': getattr(self.claim_info, 'claim_id', None),
            'total_claim_charge_amount': getattr(self.claim_info, 'claim_amount', None),
            'claim_provider_supplier_signature_indicator': getattr(self.claim_info, 'provider_supplier_signature_indicator', None),
            'claim_assignment_or_plan_participation_code': getattr(self.claim_info, 'assignment_or_plan_participation_code', None),
            'claim_benefits_assignment_certification_indicator': getattr(self.claim_info, 'benefits_assignment_certification_indicator', None),
            'claim_release_of_information_code': getattr(self.claim_info, 'release_of_information_code', None),
            'claim_reference_identification_qualifier': getattr(self.claim_info, 'reference_identification_qualifier', None),
            'claim_member_groupor_policyNumber': getattr(self.claim_info, 'member_groupor_policyNumber', None),
            'claim_description': getattr(self.claim_info, 'description', None),
            'claim_reference_identifier': getattr(self.claim_info, 'reference_identifier', None),
            'serviceline_reference_identification_qualifier': getattr(self.ref_info, 'reference_identification_qualifier', None),
            'serviceline_member_groupor_policyNumber': getattr(self.ref_info, 'member_groupor_policyNumber', None),
            'serviceline_description': getattr(self.ref_info, 'description', None),
            'serviceline_reference_identifier': getattr(self.ref_info, 'reference_identifier', None)

        }
        
        # Add flattened bht information
        if hasattr(self, 'bht_info') and self.bht_info:
            base_data.update(self.bht_info.to_denormalized_dict('bht'))

        # Add flattened hirarchy information
        if hasattr(self, 'hl_info') and self.hl_info:
            base_data.update(self.hl_info.to_denormalized_dict('hl'))

        # Add flattened patient demographics
        if hasattr(self, 'patient_info') and self.patient_info:
            base_data.update(self.patient_info.to_denormalized_dict('patient'))
        
        # Add flattened subscriber demographics (if different)
        if hasattr(self, 'subscriber_info') and self.subscriber_info != self.patient_info:
            base_data.update(self.subscriber_info.to_denormalized_dict('subscriber'))
        
        # Add flattened payer information
        if hasattr(self, 'payer_info') and self.payer_info:
            base_data.update(self.payer_info.to_denormalized_dict())
        
        # Add flattened submitter/receiver
        if hasattr(self, 'submitter_info') and self.submitter_info:
            base_data.update(self.submitter_info.to_denormalized_dict('submitter'))
        if hasattr(self, 'receiver_info') and self.receiver_info:
            base_data.update(self.receiver_info.to_denormalized_dict('receiver'))
        
        # Add flattened providers
        if hasattr(self, 'provider_info') and self.provider_info:
            for provider_type, provider_data in self.provider_info.items():
                if provider_data:
                    base_data.update(provider_data.to_denormalized_dict(provider_type))
        
        # Add JSON arrays for repeating elements
        base_data.update({
            'diagnosis_codes': self._diagnosis_to_json_array_safe(),
            'service_lines': self._service_lines_to_json_array_safe(),
        })
        
        return base_data

    def _diagnosis_to_json_array_safe(self):
        """Generate diagnosis array handling missing elements"""
        diagnosis_list = []
        diagnosis_list_updated = []
        sequence = 1
        
        # Check if diagnosis_info exists and has data
        if not hasattr(self, 'diagnosis_info') or not self.diagnosis_info:
            return []
        
        # Principal diagnosis (may be missing)
        if hasattr(self.diagnosis_info, 'principal_dx_cd') and self.diagnosis_info.principal_dx_cd:
            for dx_code in self.diagnosis_info.principal_dx_cd:
                diagnosis_list_updated.append({
                                            "healthcare_code_information": sequence,
                                            "code_list_qualifier_code": dx_code[0],
                                            "industry_code": dx_code[1]                       
                                            })
                sequence += 1
        
        return diagnosis_list_updated

    def _service_lines_to_json_array_safe(self):
        """Generate service lines handling missing elements"""
        if not hasattr(self, 'sl_info') or not self.sl_info:
            return []
        
        service_lines = []
        
        # Iterate through service lines and build the JSON structure
        for service_line in self.sl_info:
            sl = f"service_line_num_{getattr(service_line, 'claim_line_number', None)}"
            line_data = {
                f'service_line_num': getattr(service_line, 'claim_line_number', None),
                f'service_id_qualifier': getattr(service_line, 'prcdr_cd_type', None),
                f'procedure_code': getattr(service_line, 'prcdr_cd', None),
                f'charge_amount': float(getattr(service_line, 'line_chrg_amt', 0)) if getattr(service_line, 'line_chrg_amt', None) else None,
                f'service_units': float(getattr(service_line, 'units', 1)) if getattr(service_line, 'units', None) else None,
                f'unit_of_measure': getattr(service_line, 'units_measurement', None),
                f'service_date_from': getattr(service_line, 'service_date', None),
                f'service_date_to': getattr(service_line, 'service_date', None),
                f'service_date_format': getattr(service_line, 'service_date_format', None),
                f'service_time': getattr(service_line, 'service_time', None),
            }
            
            # Handle modifiers (may be missing or empty)
            modifier_string = getattr(service_line, 'modifier_cds', '')
            modifiers = modifier_string.split(',') if modifier_string else []
            line_data[f'modifiers'] = [m.strip() for m in modifiers if m.strip()] or []
            
            # Add type-specific fields
            if hasattr(service_line, 'revenue_cd'):
                line_data[f'revenue_code'] = getattr(service_line, 'revenue_cd', None)
                
            if hasattr(service_line, 'place_of_service'):
                line_data[f'place_of_service_code'] = getattr(service_line, 'place_of_service', None)
            
            if hasattr(service_line, 'dg_cd_pntr'):
                line_data[f"composite_diagnosis_code_pointer"] = { f"diagnosis_code_pointer_{i}": str(i) for i in range(1, len(service_line.dg_cd_pntr.split(":"))+1)}

            service_lines.append(line_data)
        
        return service_lines

    def claim_lines(self):
        """Extract service lines from the claim loop"""
        sl_loop = self.sl_loop
        data = list(map(lambda i: self.sl_loop[i[0]:i[1]],
                self._index_to_tuples([(i) for i,y in enumerate(self.sl_loop) if y.segment_name()=="LX"]+[len(self.sl_loop)])))
        return data

    def build(self) -> None:
        self.bht_info = self._populate_bht()
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
        self.ref_info = self._populate_ref_loop() 

class Claim837i(MedicalClaim):
    NAME = "837I"
    
    def _attending_provider(self):
        return ProviderIdentity(nm1=self._first([x for x in self.claim_loop if x.element(1) == "71"],"NM1"),
                                prv=self._first([x for x in self.claim_loop if x.element(1) == "AT"],"PRV"),
                                ref=self._first(self.claim_loop, "REF"))

    def _operating_provider(self):
        return ProviderIdentity(nm1=self._first([x for x in self.claim_loop if x.element(1) == "72"],"NM1"),
                                prv=self._first([x for x in self.claim_loop if x.element(1) == "OP"],"PRV"))

    def _other_provider(self):
        return ProviderIdentity(nm1=self._first([x for x in self.claim_loop if x.element(1) == "73"],"NM1"),
                                prv=self._first([x for x in self.claim_loop if x.element(1) == "OT"],"PRV")) 

    def _facility_provider(self):
        return ProviderIdentity(nm1=self._first([x for x in self.claim_loop if x.element(1) == "FA"],"NM1"))
        
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
                             hi = self._first([x for x in self.claim_loop if x.segment_name() == "HI" and x.element(1).startswith("DR")], "HI"),
                             ref = self._first(self.claim_loop, "REF"))

    def _populate_ref_loop(self):
        self.ref_loop = self.sl_loop and self.sl_loop[:1] or None
        return RefIdentity(ref = self._first(self.ref_loop, "REF"))

    def _populate_sl_loop(self, missing=""):
        return list(
            map(lambda s:
                ServiceLine.from_sv2(
                    sv2 = self._first(s, "SV2"),
                    lx = self._first(s, "LX"),
                    dtp = self._first(s, "DTP")
                ),self.claim_lines()))

    def to_denormalized_json(self, filename='', transaction_control_number=''):
        """837I specific denormalized output"""
        base_data = super().to_denormalized_json(filename, transaction_control_number)
        
        # Add 837I specific fields
        base_data.update({
            'type_of_bill_code': getattr(self.claim_info, 'facility_type_code', None),
            'admission_type_code': getattr(self.claim_info, 'admission_type', None),
            'admission_source_code': getattr(self.claim_info, 'admission_src_cd', None),
            'patient_status_code': getattr(self.claim_info, 'discharge_status_cd', None),
            'drg_code': getattr(self.claim_info, 'drg_cd', None),
            'medical_record_number': getattr(self.claim_info, 'encounter_id', None),

            'transaction_type': '223'
        })
        
        return base_data

class Claim837p(MedicalClaim):
    NAME = "837P"

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
                "pay_to_address": self._pay_provider(),
                "referring": self._referring_provider(),
                "rendering": self._rendering_provider(),
                "service_facility": self._service_facility_provider()
                }
    
    def _populate_sl_loop(self, missing=""):
        return list(
            map(lambda s:
                ServiceLine.from_sv1(
                    sv1 = self._first(s, "SV1"),
                    lx = self._first(s, "LX"),
                    dtp = self._first(s, "DTP")
                ), self.claim_lines()))

    def to_denormalized_json(self, filename='', transaction_control_number=''):
        """837P specific denormalized output"""
        base_data = super().to_denormalized_json(filename, transaction_control_number)
        
        # Add 837P specific fields
        base_data.update({
            'place_of_service_code': getattr(self.claim_info, 'facility_type_code', None),
            'transaction_type': '222'
        })
        
        return base_data

# Copy ClaimBuilder from original claim.py with enhanced classes
class ClaimBuilder(EDI):
    def __init__(self, trnx_type_cls, trnx_data, delim_cls=AnsiX12Delim):
        self.data = trnx_data
        self.format_cls = delim_cls
        self.trnx_cls = trnx_type_cls
        self.loop = Loop(trnx_data)

    def build_claim(self, clm_segment, idx):
        return self.trnx_cls(
            bht_loop=self.get_bht_loop(idx),
            # hl_loop=self.get_hl_loop(idx),
            sender_receiver_loop=self.get_submitter_receiver_loop(idx),
            billing_loop=self.loop.get_loop_segments(idx, "2000A"),
            subscriber_loop=self.loop.get_loop_segments(idx, "2000B"),
            patient_loop=self.loop.get_loop_segments(idx, "2000C"),
            claim_loop=self.get_claim_loop(idx),
            sl_loop=self.get_service_line_loop(idx),
        )

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

    def get_bht_loop(self, clm_idx):
        return self.get_submitter_receiver_loop(clm_idx)

    def get_hl_loop(self, clm_idx):
        return self.get_submitter_receiver_loop(clm_idx)

    def build(self):
        if self.trnx_cls.NAME in ['837I', '837P']:

            return [
                self.build_claim(seg, i) for i, seg in self.segments_by_name_index("CLM")
            ]
        return []