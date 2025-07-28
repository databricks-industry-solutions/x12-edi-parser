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

    def __init__(self, enrollment_member,
                 health_plan_loop):
        self.raw_segments = enrollment_member
        self.health_plan_segments = health_plan_loop

        self.enrollment_data = self.build_enrollment_data()
        self.health_plan_loop = self.build_health_plan()

    def build_health_plan(self):
        plans = []
        current_plan = None

        for seg in self.health_plan_segments:
            seg_type = seg._name

            if seg_type == "HD":
                if current_plan:
                    plans.append(current_plan)
                current_plan = {
                    "coverage_type_code": seg.element(3),
                    "coverage_desc": self.COVERAGE_DESC_MAPPING.get(seg.element(3), "Unknown"),
                }
            elif seg_type == "DTP" and current_plan:
                current_plan["effective_date"] = seg.element(3)
                current_plan["end_date"] = seg.element(4)

        if current_plan:
            plans.append(current_plan)

        return plans
    
    def build_enrollment_data(self):
        segments = self.raw_segments
        nm1 = self._first(segments, "NM1")
        dmg = self._first(segments, "DMG")
        per = self._first(segments, "PER")
        n3 = self._first(segments, "N3")
        n4 = self._first(segments, "N4")
        ins = self._first(segments, "INS")
        dtp = self._first(segments, "DTP")

        return {
            "enrollment_member": {
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
                }
            },
        }

    def to_json(self):
        return {
        'enrollment_member': self.enrollment_data,
        'health_plan': self.health_plan_loop
        }