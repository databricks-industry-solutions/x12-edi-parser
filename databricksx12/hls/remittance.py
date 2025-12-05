from databricksx12.hls.claim import MedicalClaim
from databricksx12.hls.identities import RemittanceIdentity, RemittanceClaimIdentity, RemittanceServiceLineIdentity
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
                 header_number_loop):
        self.trx_header_loop = trx_header_loop
        self.payer_loop = payer_loop
        self.payee_loop = payee_loop
        self.clm_loop = clm_loop
        self.trx_summary_loop = trx_summary_loop
        self.header_number_loop = header_number_loop 
        self.build()

    def build(self):
        self.trx_header_info = self.populate_trx_loop()
        self.payer_info = self.populate_payer_loop()
        self.payee_info = self.populate_payee_loop()
        self.clm_info = self.populate_claim_loop()
        self.plb_info = self.populate_plb_loop()
        self.header_info = self.populate_header_loop()

    def populate_header_loop(self): 
        identity = RemittanceIdentity(segments=self.header_number_loop)
        result = identity.to_dict()
        # Add backward compatibility field
        result['provider_id'] = self._first(self.header_number_loop, 'TS3').element(1)
        return result
    #
    # Assuming npi, date, then repeating <reason cd:id, adj amount>
    #
    def populate_plb_loop(self):
        identity = RemittanceIdentity(segments=self.trx_summary_loop)
        result = identity.to_dict()
        # Add backward compatibility fields
        plb_data = functools.reduce(lambda x, y: x+y, [
            [{
                'provider_adjustment_npi': p.element(1),
                'provider_adjustment_date': p.element(2),
                'provider_adjustment_reason_cd': p.element(i, 0),
                'provider_adjustment_id': p.element(i, 1),
                'provider_adjustment_amt': p.element(i+1)
            }
             for i in list(range(3,p.segment_len(), 2))]
            for p in self.segments_by_name("PLB", data=self.trx_summary_loop)], [])
        result['plb_adjustments'] = plb_data
        return result

    def populate_payer_loop(self):
        identity = RemittanceIdentity(segments=self.payer_loop)
        result = identity.to_dict()
        # Add backward compatibility fields
        result['entity_id_cd'] = self._first(self.payer_loop, "N1").element(1)
        result['payer_name'] = self._first(self.payer_loop, "N1").element(2)
        result['payer_street'] = self._first(self.payer_loop, "N3").element(1)
        result['payer_city'] = self._first(self.payer_loop, "N4").element(1)
        result['payer_state'] = self._first(self.payer_loop, "N4").element(2)
        result['payer_zip'] = self._first(self.payer_loop, "N4").element(3)
        result['payer_contact_info'] = [
            {
                'payer_contact_name': c.element(2),
                'payer_contact_function_cd': c.element(1),
                'payer_contact_number': c.element(6),
                'payer_email': c.element(4)
            }
            for c in self.segments_by_name("PER", data = self.payer_loop)]
        ref_seg = self._first(self.payer_loop, "REF")
        result['payer_primary_id'] = ref_seg.element(1) if ref_seg else ""
        result['payer_secondary_id'] = ref_seg.element(2) if ref_seg else ""
        return result

    def populate_payee_loop(self):
        identity = RemittanceIdentity(segments=self.payee_loop)
        result = identity.to_dict()
        # Add backward compatibility fields
        n1_seg = self._first(self.payee_loop, "N1")
        result['payee_name'] = n1_seg.element(2) if n1_seg else ""
        result['payee_npi'] = n1_seg.element(3) if n1_seg else ""
        result['payee_id_cd'] = n1_seg.element(4) if n1_seg else ""
        ref_seg = self._first(self.payee_loop, "REF")
        result['payee_tax_id'] = ref_seg.element(2) if ref_seg else ""
        return result
    
    def populate_trx_loop(self):
        identity = RemittanceIdentity(segments=self.trx_header_loop)
        result = identity.to_dict()
        # Add backward compatibility fields
        bpr_seg = self._first(self.trx_header_loop,"BPR")
        trn_seg = self._first(self.trx_header_loop,"TRN")
        result['transaction_handling_cd'] = bpr_seg.element(1) if bpr_seg else ""
        result['monetary_amt'] = bpr_seg.element(2) if bpr_seg else ""
        result['credit_debit_flag'] = bpr_seg.element(3) if bpr_seg else ""
        result['payment_method_cd'] = bpr_seg.element(4) if bpr_seg else ""
        result['payment_date'] = bpr_seg.element(16) if bpr_seg else ""
        result['trace_type_cd'] = trn_seg.element(1) if trn_seg else ""
        result['trace_reference_id'] = trn_seg.element(2) if trn_seg else ""
        result['trace_origin_company_id'] = trn_seg.element(3) if trn_seg else ""
        return result

    def populate_claim_loop(self):
        identity = RemittanceClaimIdentity(segments=self.clm_loop)
        result = identity.to_dict()
        # Add backward compatibility fields
        end_clp_index = ([i for i,z in enumerate([y._name for y in self.clm_loop[1:]]) if z == "CLP"] + [len(self.clm_loop)])[0]
        clp_seg = self._first(self.clm_loop,"CLP")
        nm1_seg = self._first(self.clm_loop,"NM1")
        result['claim_id'] = clp_seg.element(1) if clp_seg else ""
        result['person_or_organization'] = self._populate_names(self.clm_loop[:end_clp_index])
        result['claim_status_cd'] = clp_seg.element(2) if clp_seg else ""
        result['claim_chrg_amt'] = clp_seg.element(3) if clp_seg else ""
        result['claim_pay_amt'] = clp_seg.element(4) if clp_seg else ""
        result['patient_pay_amt'] = clp_seg.element(5) if clp_seg else ""
        result['claim_filing_cd'] = clp_seg.element(6) if clp_seg else ""
        result['payer_claim_id'] = clp_seg.element(7) if clp_seg else ""
        result['type_of_bill_cd'] = clp_seg.element(8) if clp_seg else ""
        result['claim_freq_cd'] = clp_seg.element(9) if clp_seg else ""
        result['drg_cd'] = clp_seg.element(11) if clp_seg else ""
        result['patient_entity_id_cd'] = nm1_seg.element(1) if nm1_seg else ""
        result['entity_type_qualifier'] = nm1_seg.element(2) if nm1_seg else ""
        result['patient_last_nm'] = nm1_seg.element(4) if nm1_seg else ""
        result['patient_first_nm'] = nm1_seg.element(5) if nm1_seg else ""
        result['id_code_qualifier'] = nm1_seg.element(8) if nm1_seg else ""
        result['patient_id'] = nm1_seg.element(9) if nm1_seg else ""
        result['clm_refs'] = [{'id_code_qualifier': x.element(1), 'id': x.element(2)}  for x in self.segments_by_name("REF", data=self.clm_loop[:self.index_of_segment(self.clm_loop, 'SVC')])]
        #Claim level service adjustments CAS
        result['service_adjustments'] = functools.reduce(lambda x,y: x+y,
            [self.populate_adjustment_groups(x)
             for x in self.segments_by_name("CAS",
                data = self.clm_loop[1:min(  list(filter(lambda x: x>=0, [self.index_of_segment(self.clm_loop, 'SVC'), len(self.clm_loop)-1]))) ])], [])
        result['claim_lines'] = [self.populate_claim_line(seg, i, min(self.index_of_segment(self.clm_loop, 'SVC', i+1), len(self.clm_loop)-1)) for i,seg in self.segments_by_name_index(segment_name="SVC", data=self.clm_loop)]
        result['date_references'] = [{'date_cd': x.element(1), 'date': x.element(2)} for x in self.clm_loop if x._name == "DTM"]
        return result

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
                 for x in loop if x._name== "NM1"]

    
    #
    # @parma svc - the svc segment for the service rendered
    # @param idx - the index where the svc is found within self.clm_loop
    # @param svc_end_idx - the last segment associated with the service 
    #
    def populate_claim_line(self, svc, idx, svc_end_idx):
        # 1. Greedy Extraction
        # Capture unmapped segments like REF (Line Item Control), AMT (Allowed Amount), LQ (Remark)
        raw_segments = self.clm_loop[idx:svc_end_idx]
        identity = RemittanceServiceLineIdentity(segments=raw_segments)
        greedy_data = identity.to_dict()

        # 2. Explicit Mapping (Keep existing business logic)
        explicit_data = {
            'prcdr_cd':svc.element(1),
            'chrg_amt':svc.element(2),
            'paid_amt':svc.element(3),
            'rev_cd':svc.element(4),
            'units': svc.element(5),
            'original_prcdr_cd':svc.element(6),
            'service_date_qualifier_cd': self._first(self.clm_loop, "DTM", idx).element(1),
            'service_date': self._first(self.clm_loop, "DTM", idx).element(2),
            'other_amts': [{'amt_qualifier_cd': a.element(1), 'amt': a.element(2)} for a in self.segments_by_name("AMT", data=self.clm_loop[idx:svc_end_idx]) ],
            'remarks': [{'qualifier_cd': x.element(1), 'remark_cd': x.element(2)} for x in self.segments_by_name("LQ", data = self.clm_loop[idx:svc_end_idx])],
            #line level service adjustments
            'service_adjustments': functools.reduce(lambda x,y: x+y,
                [self.populate_adjustment_groups(x) for x in self.segments_by_name("CAS", data =  self.clm_loop[idx:svc_end_idx])], []),
            'line_refs': [{'id_code_qualifier': x.element(1), 'id': x.element(2)} for x in  self.segments_by_name("REF", data=self.clm_loop[idx:svc_end_idx])]
        }

        # 3. Merge (Explicit overwrites greedy if keys collide)
        return {**greedy_data, **explicit_data}

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
            **{'provider_adjustments': self.plb_info},
            **{'header_info': self.header_info}
        }
    
    
