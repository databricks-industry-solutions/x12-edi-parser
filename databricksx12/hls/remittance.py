from databricksx12.hls.claim import MedicalClaim
import functools
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
                 trx_summary_loop,
                 header_number_loop,
                 include_loop_segments=False):
        self.trx_header_loop = trx_header_loop
        self.payer_loop = payer_loop
        self.payee_loop = payee_loop
        self.clm_loop = clm_loop
        self.trx_summary_loop = trx_summary_loop
        self.header_number_loop = header_number_loop
        self._include_loop_segments = include_loop_segments
        self._loop_segments_info = None
        self.build()

    def build(self):
        self.trx_header_info = self.populate_trx_loop()
        self.payer_info = self.populate_payer_loop()
        self.payee_info = self.populate_payee_loop()
        self.clm_info = self.populate_claim_loop()
        self.plb_info = self.populate_plb_loop()
        self.header_info = self.populate_header_loop()

    def _build_loop_segments_info(self):
        return {
            'trx_header_loop': self._extract_segments(self.trx_header_loop),
            'payer_loop': self._extract_segments(self.payer_loop),
            'payee_loop': self._extract_segments(self.payee_loop),
            'clm_loop': self._extract_segments(self.clm_loop),
            'trx_summary_loop': self._extract_segments(self.trx_summary_loop),
            'header_number_loop': self._extract_segments(self.header_number_loop),
        }

    @property
    def loop_segments_info(self):
        if self._loop_segments_info is None and self._include_loop_segments:
            self._loop_segments_info = self._build_loop_segments_info()
        return self._loop_segments_info or {}

    def populate_header_loop(self):
        ts3 = self._first(self.header_number_loop, 'TS3')
        return {
            'provider_id': ts3.element(1)
        }

    def populate_plb_loop(self):
        return functools.reduce(lambda x, y: x+y, [
            [{
                'provider_adjustment_npi': p.element(1),
                'provider_adjustment_date': p.element(2),
                'provider_adjustment_reason_cd': p.element(i, 0),
                'provider_adjustment_id': p.element(i, 1),
                'provider_adjustment_amt': p.element(i+1)
            }
             for i in list(range(3, p.segment_len(), 2))]
            for p in self.segments_by_name("PLB", data=self.trx_summary_loop)], [])

    def populate_payer_loop(self):
        n1 = self._first(self.payer_loop, "N1")
        n3 = self._first(self.payer_loop, "N3")
        n4 = self._first(self.payer_loop, "N4")
        ref = self._first(self.payer_loop, "REF")
        return {
            'entity_id_cd': n1.element(1),
            'payer_name': n1.element(2),
            'payer_street': n3.element(1),
            'payer_city': n4.element(1),
            'payer_state': n4.element(2),
            'payer_zip': n4.element(3),
            'payer_contact_info': [
                {
                    'payer_contact_name': c.element(2),
                    'payer_contact_function_cd': c.element(1),
                    'payer_contact_number': c.element(6),
                    'payer_email': c.element(4)
                }
                for c in self.segments_by_name("PER", data=self.payer_loop)],
            'payer_primary_id': ref.element(1),
            'payer_secondary_id': ref.element(2)
        }

    def populate_payee_loop(self):
        n1 = self._first(self.payee_loop, "N1")
        ref = self._first(self.payee_loop, "REF")
        return {
            'payee_name': n1.element(2),
            'payee_npi': n1.element(3),
            'payee_id_cd': n1.element(4),
            'payee_tax_id': ref.element(2)
        }

    def populate_trx_loop(self):
        bpr = self._first(self.trx_header_loop, "BPR")
        trn = self._first(self.trx_header_loop, "TRN")
        return {
            'transaction_handling_cd': bpr.element(1),
            'monetary_amt': bpr.element(2),
            'credit_debit_flag': bpr.element(3),
            'payment_method_cd': bpr.element(4),
            'payment_date': bpr.element(16),
            'trace_type_cd': trn.element(1),
            'trace_reference_id': trn.element(2),
            'trace_origin_company_id': trn.element(3)
        }

    def _scan_clm_loop(self):
        """Single pass over clm_loop to collect segments needed by populate_claim_loop."""
        clm_loop = self.clm_loop
        end_clp_index = len(clm_loop)
        for i in range(1, len(clm_loop)):
            if clm_loop[i]._name == "CLP":
                end_clp_index = i - 1
                break

        first_svc_idx = -1
        svc_indices = []
        dtm_segments = []
        clp_seg = None
        first_nm1 = None

        for i, seg in enumerate(clm_loop):
            name = seg._name
            if name == "CLP" and clp_seg is None:
                clp_seg = seg
            elif name == "NM1" and first_nm1 is None:
                first_nm1 = seg
            elif name == "SVC":
                if first_svc_idx < 0:
                    first_svc_idx = i
                svc_indices.append(i)
            elif name == "DTM":
                dtm_segments.append(seg)

        cas_end = min(filter(lambda x: x >= 0, [first_svc_idx, len(clm_loop) - 1]))
        cas_slice = clm_loop[1:cas_end]
        claim_cas_segments = [seg for seg in cas_slice if seg._name == "CAS"]

        ref_slice = clm_loop[:first_svc_idx] if first_svc_idx >= 0 else clm_loop[:-1]
        refs_before_svc = [seg for seg in ref_slice if seg._name == "REF"]

        return {
            'end_clp_index': end_clp_index,
            'clp_seg': clp_seg,
            'first_nm1': first_nm1,
            'refs_before_svc': refs_before_svc,
            'claim_cas_segments': claim_cas_segments,
            'svc_indices': svc_indices,
            'dtm_segments': dtm_segments,
        }

    def populate_claim_loop(self):
        scan = self._scan_clm_loop()
        clp = scan['clp_seg'] or self._first(self.clm_loop, "CLP")
        nm1 = scan['first_nm1'] or self._first(self.clm_loop, "NM1")
        clm_loop_len = len(self.clm_loop)

        claim_lines = []
        for pos, idx in enumerate(scan['svc_indices']):
            next_svc = scan['svc_indices'][pos + 1] if pos + 1 < len(scan['svc_indices']) else -1
            svc_end_idx = min(next_svc, clm_loop_len - 1)
            claim_lines.append(
                self.populate_claim_line(self.clm_loop[idx], idx, svc_end_idx)
            )

        return {
            'claim_id': clp.element(1),
            'person_or_organization': self._populate_names(self.clm_loop[:scan['end_clp_index']]),
            'claim_status_cd': clp.element(2),
            'claim_chrg_amt': clp.element(3),
            'claim_pay_amt': clp.element(4),
            'patient_pay_amt': clp.element(5),
            'claim_filing_cd': clp.element(6),
            'payer_claim_id': clp.element(7),
            'type_of_bill_cd': clp.element(8),
            'claim_freq_cd': clp.element(9),
            'drg_cd': clp.element(11),
            'patient_entity_id_cd': nm1.element(1),
            'entity_type_qualifier': nm1.element(2),
            'patient_last_nm': nm1.element(4),
            'patient_first_nm': nm1.element(5),
            'id_code_qualifier': nm1.element(8),
            'patient_id': nm1.element(9),
            'clm_refs': [
                {'id_code_qualifier': x.element(1), 'id': x.element(2)}
                for x in scan['refs_before_svc']
            ],
            'service_adjustments': functools.reduce(
                lambda x, y: x + y,
                [self.populate_adjustment_groups(x) for x in scan['claim_cas_segments']],
                []
            ),
            'claim_lines': claim_lines,
            'date_references': [
                {'date_cd': x.element(1), 'date': x.element(2)}
                for x in scan['dtm_segments']
            ],
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
            for x in loop if x._name == "NM1"]

    def populate_claim_line(self, svc, idx, svc_end_idx):
        dtm = self._first(self.clm_loop, "DTM", idx)
        line_slice = self.clm_loop[idx:svc_end_idx]
        return {
            'prcdr_cd': svc.element(1),
            'chrg_amt': svc.element(2),
            'paid_amt': svc.element(3),
            'rev_cd': svc.element(4),
            'units': svc.element(5),
            'original_prcdr_cd': svc.element(6),
            'service_date_qualifier_cd': dtm.element(1),
            'service_date': dtm.element(2),
            'other_amts': [
                {'amt_qualifier_cd': a.element(1), 'amt': a.element(2)}
                for a in line_slice if a._name == "AMT"
            ],
            'remarks': [
                {'qualifier_cd': x.element(1), 'remark_cd': x.element(2)}
                for x in line_slice if x._name == "LQ"
            ],
            'service_adjustments': functools.reduce(
                lambda x, y: x + y,
                [self.populate_adjustment_groups(x) for x in line_slice if x._name == "CAS"],
                []
            ),
            'line_refs': [
                {'id_code_qualifier': x.element(1), 'id': x.element(2)}
                for x in line_slice if x._name == "REF"
            ],
        }

    def populate_adjustment_groups(self, cas):
        return [
            {
                'adjustment_grp_cd': (cas.element(1) if cas.element(i) == "" else cas.element(i)),
                'adjustment_reason_cd': cas.element(i+1),
                'adjustment_amount': cas.element(i+2)
            }
            for i in list(range(1, cas.segment_len()-1, 3))
        ]

    def to_json(self, include_loop_segments=False):
        result = {
            **{'payment': self.trx_header_info},
            **{'payer': self.payer_info},
            **{'payee': self.payee_info},
            **{'claim': self.clm_info},
            **{'provider_adjustments': self.plb_info},
            **{'header_info': self.header_info},
        }
        if include_loop_segments or self._include_loop_segments:
            if self._loop_segments_info is None:
                self._loop_segments_info = self._build_loop_segments_info()
            result['input_loop_segments'] = self._loop_segments_info
        return result

