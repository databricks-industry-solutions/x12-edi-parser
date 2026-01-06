from databricksx12.edi import EDI, AnsiX12Delim, Segment
from databricksx12.hls.claim import MedicalClaim


class MemberEnrollment(MedicalClaim):
    NAME = "834"

    IDENTIFIER_TYPE_MAPPING = {
        "34": "Social Security Number (SSN)",
        "ZZ": "Mutually Defined Identifier",
        "XX": "National Provider Identifier (NPI)",
        "FI": "Federal Tax ID",
        "EI": "Employer ID",
        "MI": "Member ID",
        "SY": "Social Security Number (Alt)",
        "NI": "National Insurance Number"
    }

    COVERAGE_DESC_MAPPING = {
        "HLT": "Health",
        "DEN": "Dental",
        "VIS": "Vision"
    }

    def __init__(self, member_detail_loop):
        self.member_detail_loop = member_detail_loop
        self.enrollment_data = self.build_enrollment_data()

    def build_plan_elections(self, segments):
        # Build local index for O(1) lookups
        segment_index = {}
        for x in segments:
            if x._name not in segment_index:
                segment_index[x._name] = []
            segment_index[x._name].append(x)
            
        def get_dtp(qualifier):
            found = [x for x in segment_index.get("DTP", []) if x.element(1) == qualifier]
            return found[0] if found else Segment.empty()
            
        dtp_348 = get_dtp("348")
        dtp_349 = get_dtp("349")
        dtp_344 = get_dtp("344")
        dtp_345 = get_dtp("345")
        
        cob = segment_index.get("COB", [Segment.empty()])[0]

        return {
            'coverage_type_code': segments[0].element(3), #first segment is always HD
            'coverage_desc':  self.COVERAGE_DESC_MAPPING.get(segments[0].element(3), "Unknown"),
            'coverage_start_dt': dtp_348.element(3),
            'coverage_start_dt_format': dtp_348.element(2),
            'coverage_end_dt': dtp_349.element(3),
            'coverage_end_dt_format': dtp_349.element(2),
            'cob_payer_responsible_cd': cob.element(1),
            'cob_policy_number': cob.element(2),   
            'cob_indicator_cd': cob.element(3), 
            'cob_service_type_cd': cob.element(4), 
            'cob_start_dt': dtp_344.element(3),
            'cob_start_dt_format': dtp_344.element(2),
            'cob_end_dt': dtp_345.element(3),
            'cob_end_dt_format': dtp_345.element(2),
        }
    
    def build_enrollment_data(self):
        # Build local index for O(1) lookups
        segment_index = {}
        for x in self.member_detail_loop:
            if x._name not in segment_index:
                segment_index[x._name] = []
            segment_index[x._name].append(x)
            
        def get_first(name):
            return segment_index.get(name, [Segment.empty()])[0]

        nm1 = get_first("NM1")
        dmg = get_first("DMG")
        per = get_first("PER")
        n3 = get_first("N3")
        n4 = get_first("N4")
        ins = get_first("INS")
        dtp = get_first("DTP")
        
        hd_idx = [i for i, seg in self.segments_by_name_index("HD", data=self.member_detail_loop)] + [len(self.member_detail_loop)]
        
        return {
                "member_id_code": nm1.element(9),
                "member_identifier_type": self.IDENTIFIER_TYPE_MAPPING.get(nm1.element(8), nm1.element(8)),
                "member_first_name": nm1.element(4),
                "member_last_name": nm1.element(3),
                "member_dob": dmg.element(2),
                "member_gender": dmg.element(3),
                "contact_phone_number": {
                    "home_phone": per.element(4),
                    "work_phone": per.element(6)
                },
                "address": {
                    "street_name": n3.element(1),
                    "apartment_name": n3.element(2),
                    "city_name": n4.element(1),
                    "state_code": n4.element(2),
                    "postal_code": n4.element(3)
                },
                "Maintenance": {
                    "benefit_status": ins.element(1),
                    "maintenance_type_code": ins.element(3),
                    "maintenance_reason_code": ins.element(4),
                    "coverage_start_date" : dtp.element(3)
                },
                "health_coverage_elections": [
                    self.build_plan_elections(self.member_detail_loop[t[0]:t[1]]) for t in zip(hd_idx, hd_idx[1:])
                ]
            }

    def to_json(self):
        return {
            'enrollment_member': self.enrollment_data
        }