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
        return {
            'provider_id': self._first(self.header_number_loop, 'TS3').element(1)
        }
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
            'other_amts': [{'amt_qualifier_cd': a.element(1), 'amt': a.element(2)} for a in self.segments_by_name("AMT", data=self.clm_loop[idx:svc_end_idx]) ],
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
            **{'provider_adjustments': self.plb_info},
            **{'header_info': self.header_info}
        }
    
    
