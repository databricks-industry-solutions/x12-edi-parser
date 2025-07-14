from databricksx12.edi import Segment
from typing import List, Dict
from collections import defaultdict
from functools import reduce

class Identity:
    def to_dict(self):
        return {k: v for k, v in self.__dict__.items() if v is not None}

class BhtIdentity(Identity):
    def __init__(self, bht=Segment.empty()):
        if not bht.is_empty() and bht.element(0):
            self.hierarchical_structure_code = bht.element(1) if bht.element(1) else None
            self.transaction_set_purpose_code = bht.element(2) if bht.element(2) else None
            self.submitter_transaction_identifier = bht.element(3) if bht.element(3) else None
            self.transaction_set_creation_date = bht.element(4) if bht.element(4) else None
            self.transaction_set_creation_time = bht.element(5) if bht.element(5) else None
            self.transaction_type_code = bht.element(6) if bht.element(6) else None
        else:
            self.hierarchical_structure_code = None
            self.transaction_set_purpose_code = None
            self.submitter_transaction_identifier = None
            self.transaction_set_creation_date = None
            self.transaction_set_creation_time = None
            self.transaction_type_code = None

    def to_denormalized_dict(self, prefix):
        """Return flattened dictionary with provider type prefix"""
        return {
            f'{prefix}_hierarchical_structure_code': self.hierarchical_structure_code,
            f'{prefix}_transaction_set_purpose_code': self.transaction_set_purpose_code,
            f'{prefix}_submitter_transaction_identifier': self.submitter_transaction_identifier,
            f'{prefix}_transaction_set_creation_date': self.transaction_set_creation_date,
            f'{prefix}_transaction_set_creation_time': self.transaction_set_creation_time,
            f'{prefix}_transaction_type_code': self.transaction_type_code
            }

class ProviderIdentity(Identity):
    def __init__(self, hl=Segment.empty(), nm1=Segment.empty(), n3=Segment.empty(), n4=Segment.empty(), 
                 ref=Segment.empty(), prv=Segment.empty(), per=Segment.empty()):
        
        self.hl = hl
        self.per = per
        # Hierarchical components
        if not hl.is_empty() and hl.element(0):
            self.hierarchical_id_number = hl.element(1) if hl.element(1) else None
            self.hierarchical_parent_id_number = hl.element(2) if hl.element(2) else None
            self.hierarchical_level_code = hl.element(3) if hl.element(3) else None
            self.hierarchical_child_code = hl.element(4) if hl.element(4) else None

        # Name components
        self.entity_type = '2' if nm1.element(2) == '2' else '1'
        self.entity_identifier_code = nm1.element(1) if nm1.element(1) else None  # TODO
        self.entity_type_qualifier = self.entity_type
        self.name = nm1.element(3) if nm1.element(3) else None
        self.first_name = nm1.element(4) if self.entity_type == '1' and nm1.element(4) else None
        self.last_name = nm1.element(3) if self.entity_type == '1' and nm1.element(3) else None
        self.middle_initial = nm1.element(5) if self.entity_type == '1' and nm1.element(5) else None
        self.name_suffix = nm1.element(7) if self.entity_type == '1' and nm1.element(7) else None
        
        # Identifiers
        self.npi = nm1.element(9) if nm1.element(9) else None
        self.id_qualifier = nm1.element(8) if nm1.element(8) else None
        self.taxonomy_code = prv.element(3) if not prv.is_empty() and prv.element(3) else None
        self.provider_role = prv.element(1) if not prv.is_empty() and prv.element(1) else None
        self.specialty_code = prv.element(2) if not prv.is_empty() and prv.element(2) else None
        
        # Tax information
        self.tax_id = ref.element(2) if not ref.is_empty() and ref.element(1) == 'EI' else None
        self.tax_id_qualifier = ref.element(1) if not ref.is_empty() else None
        self.state_license_number = ref.element(2) if not ref.is_empty() and ref.element(1) == '0B' else None
        self.upin = ref.element(2) if not ref.is_empty() and ref.element(1) == '1G' else None
        self.commercial_number = ref.element(2) if not ref.is_empty() and ref.element(1) == 'G2' else None
        
        # Address
        self.address_line1 = n3.element(1) if not n3.is_empty() and n3.element(1) else None
        self.address_line2 = n3.element(2) if not n3.is_empty() and n3.element(2) else None
        self.city = n4.element(1) if not n4.is_empty() and n4.element(1) else None
        self.state = n4.element(2) if not n4.is_empty() and n4.element(2) else None
        self.zip_code = n4.element(3) if not n4.is_empty() and n4.element(3) else None
        self.country_code = n4.element(4) if not n4.is_empty() and n4.element(4) else None
        
        # Contact info
        self.phone_number = self._extract_contact_value(per, 'TE')
        self.fax_number = self._extract_contact_value(per, 'FX')
        self.email = self._extract_contact_value(per, 'EM')

        # PER segment for provider role
        if not per.is_empty() and per.element(0):
            self.contact_function_code = per.element(1) if not per.is_empty() and per.element(1) else None
            self.response_contact_name = per.element(2) if not per.is_empty() and per.element(2) else None
            self.communication_qualifier = per.element(3) if not per.is_empty() and per.element(3) else None
            self.response_communication_number = per.element(4) if not per.is_empty() and per.element(4) else None


        
    def _extract_contact_value(self, per_segments, qualifier):
        """Extract contact value by qualifier, handle missing"""
        if isinstance(per_segments, list):
            for per in per_segments:
                if not per.is_empty() and per.element(3) == qualifier:
                    return per.element(4)
        elif not per_segments.is_empty() and per_segments.element(3) == qualifier:
            return per_segments.element(4)
        return None
        
    def to_denormalized_dict(self, prefix):
        """Return flattened dictionary with provider type prefix"""
        data = {}
        if not self.hl.is_empty() and self.hl.element(0):
            data.update({
            f'{prefix}_hierarchical_id_number': self.hierarchical_id_number,
            f'{prefix}_hierarchical_parent_id_number': self.hierarchical_parent_id_number,
            f'{prefix}_hierarchical_level_code': self.hierarchical_level_code,
            f'{prefix}_hierarchical_child_code': self.hierarchical_child_code
            })
        
        if not self.per.is_empty() and self.per.element(0):
            data.update({
                f'{prefix}_contact_function_code': self.contact_function_code,
                f'{prefix}_response_contact_name': self.response_contact_name,
                f'{prefix}_communication_qualifier': self.communication_qualifier,
                f'{prefix}_response_communication_number': self.response_communication_number
            })

        # Add provider specific fields
        data.update({
            f'{prefix}_provider_npi': self.npi,
            f'{prefix}_provider_name': self.name,
            f'{prefix}_provider_first_name': self.first_name,
            f'{prefix}_provider_last_name': self.last_name,
            f'{prefix}_provider_middle_initial': self.middle_initial,
            f'{prefix}_provider_name_suffix': self.name_suffix,
            f'{prefix}_provider_entity_type': self.entity_type,
            f'{prefix}_provider_entity_identifier_code': self.entity_identifier_code,
            f'{prefix}_provider_entity_type_qualifier': self.entity_type_qualifier,
            f'{prefix}_provider_taxonomy_code': self.taxonomy_code,
            f'{prefix}_provider_code': self.provider_role,
            f'{prefix}_provider_specialty_code': self.specialty_code,
            f'{prefix}_provider_tax_id': self.tax_id,
            f'{prefix}_provider_tax_id_qualifier': self.tax_id_qualifier,
            f'{prefix}_provider_state_license_number': self.state_license_number,
            f'{prefix}_provider_upin': self.upin,
            f'{prefix}_provider_commercial_number': self.commercial_number,
            f'{prefix}_provider_address_line1': self.address_line1,
            f'{prefix}_provider_address_line2': self.address_line2,
            f'{prefix}_provider_city': self.city,
            f'{prefix}_provider_state': self.state,
            f'{prefix}_provider_zip_code': self.zip_code,
            f'{prefix}_provider_country_code': self.country_code,
            f'{prefix}_provider_phone_number': self.phone_number,
            f'{prefix}_provider_fax_number': self.fax_number,
            f'{prefix}_provider_email': self.email
        })
        return data

class PayerIdentity(Identity):
    def __init__(self, nm1, n3=Segment.empty(), n4=Segment.empty(), per=Segment.empty(), ref=Segment.empty()):
        self.name = nm1.element(3) if nm1.element(3) else None
        self.id = nm1.element(9) if nm1.element(9) else None
        self.id_qualifier = nm1.element(8) if nm1.element(8) else None
        self.entity_type = nm1.element(2) if nm1.element(2) else None
        self.entity_type_code = nm1.element(1) if nm1.element(1) else None  # TODO

        
        # Address
        self.address_line1 = n3.element(1) if not n3.is_empty() and n3.element(1) else None
        self.address_line2 = n3.element(2) if not n3.is_empty() and n3.element(2) else None
        self.city = n4.element(1) if not n4.is_empty() and n4.element(1) else None
        self.state = n4.element(2) if not n4.is_empty() and n4.element(2) else None
        self.zip_code = n4.element(3) if not n4.is_empty() and n4.element(3) else None
        self.country_code = n4.element(4) if not n4.is_empty() and n4.element(4) else None
        
        # Contact
        self.phone_number = per.element(4) if not per.is_empty() and per.element(3) == 'TE' else None
        self.fax_number = per.element(4) if not per.is_empty() and per.element(3) == 'FX' else None
        
    def to_denormalized_dict(self):
        return {
            'payer_name': self.name,
            'payer_id': self.id,
            'payer_id_qualifier': self.id_qualifier,
            'payer_entity_type': self.entity_type,
            'payer_entity_code': self.entity_type_code,
            'payer_address_line1': self.address_line1,
            'payer_address_line2': self.address_line2,
            'payer_city': self.city,
            'payer_state': self.state,
            'payer_zip_code': self.zip_code,
            'payer_country_code': self.country_code,
            'payer_phone_number': self.phone_number,
            'payer_fax_number': self.fax_number
        }

class PatientIdentity(Identity):
    def __init__(self, hl, nm1, n3, n4, dmg, pat, sbr, ref=Segment.empty(), per=Segment.empty()):
        
        self.hl = hl
        # Hierarchical components
        if not hl.is_empty() and hl.element(0):
            self.hierarchical_id_number = hl.element(1) if hl.element(1) else None
            self.hierarchical_parent_id_number = hl.element(2) if hl.element(2) else None
            self.hierarchical_level_code = hl.element(3) if hl.element(3) else None
            self.hierarchical_child_code = hl.element(4) if hl.element(4) else None
        
        # Name components
        self.entity_identifier_code = nm1.element(1) if nm1.element(1) else None
        self.entity_type_qualifier = nm1.element(2) if nm1.element(2) else None
        self.first_name = nm1.element(4) if nm1.element(4) else None
        self.last_name = nm1.element(3) if nm1.element(3) else None
        self.middle_initial = nm1.element(5) if nm1.element(5) else None
        self.name_suffix = nm1.element(7) if nm1.element(7) else None
        
        # Demographics
        self.date_of_birth = dmg.element(2) if not dmg.is_empty() and dmg.element(2) else None
        self.dob_format = dmg.element(1) if not dmg.is_empty() and dmg.element(1) else None
        self.gender_code = dmg.element(3) if not dmg.is_empty() and dmg.element(3) else None
        self.marital_status = dmg.element(4) if not dmg.is_empty() and dmg.element(4) else None
        
        # Identifiers
        self.response_contact_identifier = nm1.element(9) if nm1.element(9) else None
        self.identification_code_qualifier = nm1.element(8) if nm1.element(8) else None
        self.ssn = self._extract_ref_value(ref, 'SY')
        self.member_id = self._extract_ref_value(ref, 'MI')
        self.medical_record_number = self._extract_ref_value(ref, 'EA')
        self.account_number = self._extract_ref_value(ref, 'EJ')
        
        # Address
        self.address_line1 = n3.element(1) if not n3.is_empty() and n3.element(1) else None
        self.address_line2 = n3.element(2) if not n3.is_empty() and n3.element(2) else None
        self.city = n4.element(1) if not n4.is_empty() and n4.element(1) else None
        self.state = n4.element(2) if not n4.is_empty() and n4.element(2) else None
        self.zip_code = n4.element(3) if not n4.is_empty() and n4.element(3) else None
        self.country_code = n4.element(4) if not n4.is_empty() and n4.element(4) else None
        
        # Relationship codes
        self.patient_relationship_code = pat.element(1) if not pat.is_empty() and pat.element(1) else None
        self.subscriber_relationship_code = sbr.element(2) if not sbr.is_empty() and sbr.element(2) else None
        self.payer_responsibility_sequenceNumber_code = sbr.element(4) if not sbr.is_empty() and sbr.element(4) else None
        self.claim_filing_indicator_code = sbr.element(9) if not sbr.is_empty() and sbr.element(4) else None
        
        # Contact info
        self.phone_number = self._extract_contact_value(per, 'TE')
        self.fax_number = self._extract_contact_value(per, 'FX')
        self.email = self._extract_contact_value(per, 'EM')
        
        # Additional demographics
        self.employment_status = pat.element(2) if not pat.is_empty() and pat.element(2) else None
        self.student_status = pat.element(3) if not pat.is_empty() and pat.element(3) else None
        self.death_date = pat.element(5) if not pat.is_empty() and pat.element(5) else None
        
    def _extract_ref_value(self, ref_segments, qualifier):
        """Extract reference value by qualifier, handle missing"""
        if isinstance(ref_segments, list):
            for ref in ref_segments:
                if not ref.is_empty() and ref.element(1) == qualifier:
                    return ref.element(2)
        elif not ref_segments.is_empty() and ref_segments.element(1) == qualifier:
            return ref_segments.element(2)
        return None
        
    def _extract_contact_value(self, per_segments, qualifier):
        """Extract contact value by qualifier, handle missing"""
        if isinstance(per_segments, list):
            for per in per_segments:
                if not per.is_empty() and per.element(3) == qualifier:
                    return per.element(4)
        elif not per_segments.is_empty() and per_segments.element(3) == qualifier:
            return per_segments.element(4)
        return None

    def to_denormalized_dict(self, prefix='patient'):
        """Return flattened dictionary with prefix"""
        data = {}
        if not self.hl.is_empty() and self.hl.element(0):
            data.update({
            f'{prefix}_hierarchical_id_number': self.hierarchical_id_number,
            f'{prefix}_hierarchical_parent_id_number': self.hierarchical_parent_id_number,
            f'{prefix}_hierarchical_level_code': "23" if prefix == 'patient' else "22",
            f'{prefix}_hierarchical_child_code': self.hierarchical_child_code
            })
        
        data.update({
            f'{prefix}_entity_identifier_code': self.entity_identifier_code,
            f'{prefix}_entity_type_qualifier': self.entity_type_qualifier,
            f'{prefix}_first_name': self.first_name,
            f'{prefix}_last_name': self.last_name,
            f'{prefix}_middle_initial': self.middle_initial,
            f'{prefix}_name_suffix': self.name_suffix,
            f'{prefix}_identification_code_qualifier': self.identification_code_qualifier,
            f'{prefix}_response_contact_identifier': self.response_contact_identifier,
            f'{prefix}_date_of_birth': self.date_of_birth,
            f'{prefix}_dob_format': self.dob_format,
            f'{prefix}_gender_code': self.gender_code,
            f'{prefix}_marital_status': self.marital_status,
            f'{prefix}_ssn': self.ssn,
            f'{prefix}_member_id': self.member_id,
            f'{prefix}_medical_record_number': self.medical_record_number,
            f'{prefix}_account_number': self.account_number,
            f'{prefix}_address_line1': self.address_line1,
            f'{prefix}_address_line2': self.address_line2,
            f'{prefix}_city': self.city,
            f'{prefix}_state': self.state,
            f'{prefix}_zip_code': self.zip_code,
            f'{prefix}_country_code': self.country_code,
            f'{prefix}_phone_number': self.phone_number,
            f'{prefix}_fax_number': self.fax_number,
            f'{prefix}_email': self.email,
            f'{prefix}_relationship_code': self.patient_relationship_code if prefix=='patient' else self.subscriber_relationship_code,
            f'{prefix}_payer_responsibility_sequenceNumber_code': self.payer_responsibility_sequenceNumber_code,
            f'{prefix}_claim_filing_indicator_code': self.claim_filing_indicator_code,
            f'{prefix}_employment_status': self.employment_status,
            f'{prefix}_student_status': self.student_status,
            f'{prefix}_death_date': self.death_date
        })
        return data

class Submitter_Receiver_Identity(Identity):
    def __init__(self, nm1=Segment.empty(), per=Segment.empty()):
        self.type = 'Organization' if nm1.element(2) == '2' else 'Individual'
        self.entity_type_qualifier = nm1.element(2) if nm1.element(2) else None
        self.entity_identifier_code = nm1.element(1) if nm1.element(1) else None
        self.name = nm1.element(3) if self.type == 'Organization' else ' '.join([nm1.element(3), nm1.element(4) or "", nm1.element(5) or ""])
        self.id = nm1.element(9) if nm1.element(9) else None
        self.id_qualifier = nm1.element(8) if nm1.element(8) else None
        
        if not per.is_empty() and per.element(0):
            self.contact_function_code = per.element(1) if per.element(1) else None
            self.response_contact_name = per.element(2) if per.element(2) else None
            self.communication_qualifier = per.element(3) if per.element(3)  else None
            self.communication_qualifier_number = per.element(4) if per.element(3) else None

            # self.communication_qualifier_email = per.element(3) if per.element(3) == 'EM' else None
            # self.email = per.element(4) if per.element(3) == 'EM' else None
            # self.communication_qualifier_phone_number = per.element(5) if per.element(5) == 'TE' else None
            # self.phone_number = per.element(6) if per.element(5) == 'TE' else None
            # self.communication_qualifier_fax_number = per.element(8) if per.element(7) == 'FX' else None
            # self.fax_number = per.element(8) if per.element(7) == 'FX' else None

        else:
            self.contact_function_code =  None
            self.response_contact_name =  None
            self.communication_qualifier =  None
            self.communication_qualifier_number =  None

            # self.communication_qualifier_email =  None
            # self.email =  None
            # self.communication_qualifier_phone_number = None
            # self.phone_number =  None
            # self.communication_qualifier_fax_number =  None
            # self.fax_number =  None
            
    def to_denormalized_dict(self, prefix):
        return {
            f'{prefix}_name': self.name,
            f'{prefix}_entity_type': self.type,
            f'{prefix}_entity_type_qualifier': self.entity_type_qualifier,
            f'{prefix}_entity_identifier_code': self.entity_identifier_code,
            f'{prefix}_id': self.id,
            f'{prefix}_id_qualifier': self.id_qualifier,
            f'{prefix}_contact_function_code': self.contact_function_code,
            f'{prefix}_response_contact_name': self.response_contact_name,
            f'{prefix}_communication_qualifier': self.communication_qualifier,
            f'{prefix}_communication_qualifier_number': self.communication_qualifier_number,

            # f'{prefix}_communication_qualifier_email': self.communication_qualifier_email,
            # f'{prefix}_email': self.email,
            # f'{prefix}_communication_qualifier_phone_number': self.communication_qualifier_phone_number,
            # f'{prefix}_phone_number': self.phone_number,
            # f'{prefix}_communication_qualifier_fax_number': self.communication_qualifier_fax_number,
            # f'{prefix}_fax_number': self.fax_number
        }

class ClaimIdentity(Identity):
    def __init__(self, clm, dtp, cl1=Segment.empty(), k3=Segment.empty(), hi=Segment.empty(), ref=Segment.empty()):
        self.claim_id = clm.element(1) if clm.element(1) else None
        self.claim_amount = clm.element(2) if clm.element(2) else None
        self.facility_type_code = clm.element(5) if clm.element(5) else None

        # self.claim_dates = {'facility_type_code': s.element(1), 'facility_code_qualifier': s.element(2), 'claim_frequency_type_code': s.element(3)} for s in clm.element(5)}
        
        self.admission_type = cl1.element(1) if not cl1.is_empty() and cl1.element(1) else None
        self.admission_src_cd = cl1.element(2) if not cl1.is_empty() and cl1.element(2) else None
        self.discharge_status_cd = cl1.element(3) if not cl1.is_empty() and cl1.element(3) else None
        self.encounter_id = k3.element(1) if not k3.is_empty() and k3.element(1) else None
        self.drg_cd = hi.element(1) if not hi.is_empty() and hi.element(1) else None

        self.healthcare_service_location_information = [{'date_cd': s.element(1), 'date_format': s.element(2), 'date': s.element(3)} for s in dtp if not s.is_empty()]
        self.provider_supplier_signature_indicator = clm.element(6) if clm.element(6) else None
        self.assignment_or_plan_participation_code = clm.element(7) if clm.element(7) else None
        self.benefits_assignment_certification_indicator = clm.element(8) if clm.element(8) else None
        self.release_of_information_code = clm.element(9) if clm.element(9) else None

        self.reference_identification_qualifier = ref.element(1) if ref.element(1) else None
        self.member_groupor_policyNumber = ref.element(2) if ref.element(2) else None
        self.description = ref.element(3) if ref.element(3) else None
        self.reference_identifier = ref.element(4) if ref.element(4) else None

class RefIdentity(Identity):
    def __init__(self, ref=Segment.empty()):
        self.reference_identification_qualifier = ref.element(1) if ref.element(1) else None
        self.member_groupor_policyNumber = ref.element(2) if ref.element(2) else None
        self.description = ref.element(3) if ref.element(3) else None
        self.reference_identifier = ref.element(4) if ref.element(4) else None

class DiagnosisIdentity(Identity):
    def __init__(self, hi_segments):
        self.principal_dx_cd = []
        if hi_segments:
            self.principal_dx_cd = [s.split(":") for s in hi_segments[0].data.split("*")[1:]]


class ServiceLine(Identity):
    def __init__(self, d):
        for k,v in d.items():
            setattr(self,k,v)

    @staticmethod
    def common(sv, lx, dtp):
        return {
            "claim_line_number": lx.element(1) if lx.element(1) else None,
            "service_date": dtp.element(3) if not dtp.is_empty() and dtp.element(3) else None,
            "service_time": dtp.element(1) if not dtp.is_empty() and dtp.element(1) else None,
            "service_date_format": dtp.element(2) if not dtp.is_empty() and dtp.element(2) else None
        }

    @classmethod
    def from_sv2(cls, sv2, lx, dtp):
        return cls({**cls.common(sv2, lx, dtp),
                    **{
                        "units": sv2.element(5) if sv2.element(5) else None,
                        "units_measurement": sv2.element(4) if sv2.element(4) else None,
                        "line_chrg_amt": sv2.element(3) if sv2.element(3) else None,
                        "prcdr_cd": sv2.element(2, 1, "") if sv2.element(2, 1, "") else None,
                        "prcdr_cd_type": sv2.element(2, 0, "") if sv2.element(2, 0, "") else None,
                        "modifier_cds": ','.join(filter(lambda x: x!="", [sv2.element(2, 2, ""), sv2.element(2, 3, ""), sv2.element(2, 4,""), sv2.element(2, 5, "")])),
                        "revenue_cd": sv2.element(1) if sv2.element(1) else None,
                        "dg_cd_pntr": sv2.element(5) if sv2.element(5) else None,
                    }
                })

    @classmethod
    def from_sv1(cls, sv1, lx, dtp):
        return cls({**cls.common(sv1, lx, dtp),
                    **{
                        "units": sv1.element(4) if sv1.element(4) else None,
                        "units_measurement": sv1.element(3) if sv1.element(3) else None,
                        "line_chrg_amt": sv1.element(2) if sv1.element(2) else None,
                        "prcdr_cd": sv1.element(1, 1) if sv1.element(1, 1) else None,
                        "prcdr_cd_type": sv1.element(1, 0) if sv1.element(1, 0) else None,
                        "modifier_cds": ','.join(filter(lambda x: x!="", [sv1.element(1, 2, ""), sv1.element(1, 3, ""), sv1.element(1, 4,""), sv1.element(1, 5, "")])),
                        "place_of_service": sv1.element(5) if sv1.element(5) else None,
                        "dg_cd_pntr": sv1.element(7) if sv1.element(7) else None,
                    }
                })