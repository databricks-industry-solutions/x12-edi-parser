# Databricks notebook source
# MAGIC %md # Enhanced Denormalized 837I and 837P Processing

## COMMAND ----------

## DBTITLE 1,Install package and imports
### MAGIC %pip install git+https://github.com/databricks-industry-solutions/x12-edi-parser.git

# COMMAND ----------

from databricksx12 import *
from databricksx12.hls.healthcare_enhanced import *
from databricksx12.hls.claim_enhanced import *
from databricksx12.hls.identities_enhanced import *
import json, os
from pyspark.sql.functions import input_file_name, col, lit, size
from pyspark.sql.types import *

## COMMAND ----------

# DBTITLE 1,Spark Configuration for Optimal Performance
# Configure Spark for optimal JSON processing
# spark.conf.set("spark.sql.adaptive.enabled", "true")
# spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
# spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
# spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
# spark.conf.set("spark.driver.maxResultSize", "2g")
# spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

# COMMAND ----------

# DBTITLE 1,Schema Definitions for 837I and 837P
def get_837i_schema():
    """Schema for 837I Institutional claims"""
    return StructType([
    StructField("filename", StringType(), True),
    StructField("transaction_control_number", StringType(), True),
    StructField("claim_id", StringType(), True),
    StructField("total_claim_charge_amount", StringType(), True),
    StructField("claim_provider_supplier_signature_indicator", StringType(), True),
    StructField("claim_assignment_or_plan_participation_code", StringType(), True),
    StructField("claim_benefits_assignment_certification_indicator", StringType(), True),
    StructField("claim_release_of_information_code", StringType(), True),
    StructField("claim_reference_identification_qualifier", StringType(), True),
    StructField("claim_member_groupor_policyNumber", StringType(), True),
    StructField("claim_description", StringType(), True),
    StructField("claim_reference_identifier", StringType(), True),
    StructField("serviceline_reference_identification_qualifier", StringType(), True),
    StructField("serviceline_member_groupor_policyNumber", StringType(), True),
    StructField("serviceline_description", StringType(), True),
    StructField("serviceline_reference_identifier", StringType(), True),
    StructField("place_of_service_code", StringType(), True),
    StructField("transaction_type", StringType(), True),
    
    StructField("bht_transaction_set_purpose_code", StringType(), True),
    StructField("bht_submitter_transaction_identifier", StringType(), True),
    StructField("bht_transaction_set_creation_date", StringType(), True),
    StructField("bht_transaction_set_creation_time", StringType(), True),
    StructField("bht_transaction_type_code", StringType(), True),
    StructField("patient_hierarchical_id_number", StringType(), True),
    StructField("patient_hierarchical_parent_id_number", StringType(), True),
    StructField("patient_hierarchical_level_code", StringType(), True),
    StructField("patient_hierarchical_child_code", StringType(), True),
    StructField("patient_entity_identifier_code", StringType(), True),
    StructField("patient_entity_type_qualifier", StringType(), True),
    StructField("patient_first_name", StringType(), True),
    StructField("patient_last_name", StringType(), True),
    StructField("patient_middle_initial", StringType(), True),
    StructField("patient_name_suffix", StringType(), True),
    StructField("patient_identification_code_qualifier", StringType(), True),
    StructField("patient_response_contact_identifier", StringType(), True),
    StructField("patient_date_of_birth", StringType(), True),
    StructField("patient_dob_format", StringType(), True),
    StructField("patient_gender_code", StringType(), True),
    StructField("patient_marital_status", StringType(), True),
    StructField("patient_ssn", StringType(), True),
    StructField("patient_member_id", StringType(), True),
    StructField("patient_medical_record_number", StringType(), True),
    StructField("patient_account_number", StringType(), True),
    StructField("patient_address_line1", StringType(), True),
    StructField("patient_address_line2", StringType(), True),
    StructField("patient_city", StringType(), True),
    StructField("patient_state", StringType(), True),
    StructField("patient_zip_code", StringType(), True),
    StructField("patient_country_code", StringType(), True),
    StructField("patient_phone_number", StringType(), True),
    StructField("patient_fax_number", StringType(), True),
    StructField("patient_email", StringType(), True),
    StructField("patient_relationship_code", StringType(), True),
    StructField("patient_payer_responsibility_sequenceNumber_code", StringType(), True),
    StructField("patient_claim_filing_indicator_code", StringType(), True),
    StructField("patient_employment_status", StringType(), True),
    StructField("patient_student_status", StringType(), True),
    StructField("patient_death_date", StringType(), True),
    StructField("subscriber_hierarchical_id_number", StringType(), True),
    StructField("subscriber_hierarchical_parent_id_number", StringType(), True),
    StructField("subscriber_hierarchical_level_code", StringType(), True),
    StructField("subscriber_hierarchical_child_code", StringType(), True),
    StructField("subscriber_entity_identifier_code", StringType(), True),
    StructField("subscriber_entity_type_qualifier", StringType(), True),
    StructField("subscriber_first_name", StringType(), True),
    StructField("subscriber_last_name", StringType(), True),
    StructField("subscriber_middle_initial", StringType(), True),
    StructField("subscriber_name_suffix", StringType(), True),
    StructField("subscriber_identification_code_qualifier", StringType(), True),
    StructField("subscriber_response_contact_identifier", StringType(), True),
    StructField("subscriber_date_of_birth", StringType(), True),
    StructField("subscriber_dob_format", StringType(), True),
    StructField("subscriber_gender_code", StringType(), True),
    StructField("subscriber_marital_status", StringType(), True),
    StructField("subscriber_ssn", StringType(), True),
    StructField("subscriber_member_id", StringType(), True),
    StructField("subscriber_medical_record_number", StringType(), True),
    StructField("subscriber_account_number", StringType(), True),
    StructField("subscriber_address_line1", StringType(), True),
    StructField("subscriber_address_line2", StringType(), True),
    StructField("subscriber_city", StringType(), True),
    StructField("subscriber_state", StringType(), True),
    StructField("subscriber_zip_code", StringType(), True),
    StructField("subscriber_country_code", StringType(), True),
    StructField("subscriber_phone_number", StringType(), True),
    StructField("subscriber_fax_number", StringType(), True),
    StructField("subscriber_email", StringType(), True),
    StructField("subscriber_relationship_code", StringType(), True),
    StructField("subscriber_payer_responsibility_sequenceNumber_code", StringType(), True),
    StructField("subscriber_claim_filing_indicator_code", StringType(), True),
    StructField("subscriber_employment_status", StringType(), True),
    StructField("subscriber_student_status", StringType(), True),
    StructField("subscriber_death_date", StringType(), True),
    StructField("payer_name", StringType(), True),
    StructField("payer_id", StringType(), True),
    StructField("payer_id_qualifier", StringType(), True),
    StructField("payer_entity_type", StringType(), True),
    StructField("payer_entity_code", StringType(), True),
    StructField("payer_address_line1", StringType(), True),
    StructField("payer_address_line2", StringType(), True),
    StructField("payer_city", StringType(), True),
    StructField("payer_state", StringType(), True),
    StructField("payer_zip_code", StringType(), True),
    StructField("payer_country_code", StringType(), True),
    StructField("payer_phone_number", StringType(), True),
    StructField("payer_fax_number", StringType(), True),
    StructField("submitter_name", StringType(), True),
    StructField("submitter_entity_type", StringType(), True),
    StructField("submitter_entity_type_qualifier", StringType(), True),
    StructField("submitter_entity_identifier_code", StringType(), True),
    StructField("submitter_id", StringType(), True),
    StructField("submitter_id_qualifier", StringType(), True),
    StructField("submitter_contact_function_code", StringType(), True),
    StructField("submitter_response_contact_name", StringType(), True),
    StructField("submitter_communication_qualifier", StringType(), True),
    StructField("submitter_communication_qualifier_number", StringType(), True),
    StructField("receiver_name", StringType(), True),
    StructField("receiver_entity_type", StringType(), True),
    StructField("receiver_entity_type_qualifier", StringType(), True),
    StructField("receiver_entity_identifier_code", StringType(), True),
    StructField("receiver_id", StringType(), True),
    StructField("receiver_id_qualifier", StringType(), True),
    StructField("receiver_contact_function_code", StringType(), True),
    StructField("receiver_response_contact_name", StringType(), True),
    StructField("receiver_communication_qualifier", StringType(), True),
    StructField("receiver_communication_qualifier_number", StringType(), True),
    StructField("billing_hierarchical_id_number", StringType(), True),
    StructField("billing_hierarchical_parent_id_number", StringType(), True),
    StructField("billing_hierarchical_level_code", StringType(), True),
    StructField("billing_hierarchical_child_code", StringType(), True),
    StructField("billing_provider_npi", StringType(), True),
    StructField("billing_provider_name", StringType(), True),
    StructField("billing_provider_first_name", StringType(), True),
    StructField("billing_provider_last_name", StringType(), True),
    StructField("billing_provider_middle_initial", StringType(), True),
    StructField("billing_provider_name_suffix", StringType(), True),
    StructField("billing_provider_entity_type", StringType(), True),
    StructField("billing_provider_entity_identifier_code", StringType(), True),
    StructField("billing_provider_entity_type_qualifier", StringType(), True),
    StructField("billing_provider_taxonomy_code", StringType(), True),
    StructField("billing_provider_code", StringType(), True),
    StructField("billing_provider_specialty_code", StringType(), True),
    StructField("billing_provider_tax_id", StringType(), True),
    StructField("billing_provider_tax_id_qualifier", StringType(), True),
    StructField("billing_provider_state_license_number", StringType(), True),
    StructField("billing_provider_upin", StringType(), True),
    StructField("billing_provider_commercial_number", StringType(), True),
    StructField("billing_provider_address_line1", StringType(), True),
    StructField("billing_provider_address_line2", StringType(), True),
    StructField("billing_provider_city", StringType(), True),
    StructField("billing_provider_state", StringType(), True),
    StructField("billing_provider_zip_code", StringType(), True),
    StructField("billing_provider_country_code", StringType(), True),
    StructField("billing_provider_phone_number", StringType(), True),
    StructField("billing_provider_fax_number", StringType(), True),
    StructField("billing_provider_email", StringType(), True),
    StructField("attending_provider_npi", StringType(), True),
    StructField("attending_provider_name", StringType(), True),
    StructField("attending_provider_first_name", StringType(), True),
    StructField("attending_provider_last_name", StringType(), True),
    StructField("attending_provider_middle_initial", StringType(), True),
    StructField("attending_provider_name_suffix", StringType(), True),
    StructField("attending_provider_entity_type", StringType(), True),
    StructField("attending_provider_entity_identifier_code", StringType(), True),
    StructField("attending_provider_entity_type_qualifier", StringType(), True),
    StructField("attending_provider_taxonomy_code", StringType(), True),
    StructField("attending_provider_code", StringType(), True),
    StructField("attending_provider_specialty_code", StringType(), True),
    StructField("attending_provider_tax_id", StringType(), True),
    StructField("attending_provider_tax_id_qualifier", StringType(), True),
    StructField("attending_provider_state_license_number", StringType(), True),
    StructField("attending_provider_upin", StringType(), True),
    StructField("attending_provider_commercial_number", StringType(), True),
    StructField("attending_provider_address_line1", StringType(), True),
    StructField("attending_provider_address_line2", StringType(), True),
    StructField("attending_provider_city", StringType(), True),
    StructField("attending_provider_state", StringType(), True),
    StructField("attending_provider_zip_code", StringType(), True),
    StructField("attending_provider_country_code", StringType(), True),
    StructField("attending_provider_phone_number", StringType(), True),
    StructField("attending_provider_fax_number", StringType(), True),
    StructField("attending_provider_email", StringType(), True),
    StructField("operating_provider_npi", StringType(), True),
    StructField("operating_provider_name", StringType(), True),
    StructField("operating_provider_first_name", StringType(), True),
    StructField("operating_provider_last_name", StringType(), True),
    StructField("operating_provider_middle_initial", StringType(), True),
    StructField("operating_provider_name_suffix", StringType(), True),
    StructField("operating_provider_entity_type", StringType(), True),
    StructField("operating_provider_entity_identifier_code", StringType(), True),
    StructField("operating_provider_entity_type_qualifier", StringType(), True),
    StructField("operating_provider_taxonomy_code", StringType(), True),
    StructField("operating_provider_code", StringType(), True),
    StructField("operating_provider_specialty_code", StringType(), True),
    StructField("operating_provider_tax_id", StringType(), True),
    StructField("operating_provider_tax_id_qualifier", StringType(), True),
    StructField("operating_provider_state_license_number", StringType(), True),
    StructField("operating_provider_upin", StringType(), True),
    StructField("operating_provider_commercial_number", StringType(), True),
    StructField("operating_provider_address_line1", StringType(), True),
    StructField("operating_provider_address_line2", StringType(), True),
    StructField("operating_provider_city", StringType(), True),
    StructField("operating_provider_state", StringType(), True),
    StructField("operating_provider_zip_code", StringType(), True),
    StructField("operating_provider_country_code", StringType(), True),
    StructField("operating_provider_phone_number", StringType(), True),
    StructField("operating_provider_fax_number", StringType(), True),
    StructField("operating_provider_email", StringType(), True),
    StructField("other_provider_npi", StringType(), True),
    StructField("other_provider_name", StringType(), True),
    StructField("other_provider_first_name", StringType(), True),
    StructField("other_provider_last_name", StringType(), True),
    StructField("other_provider_middle_initial", StringType(), True),
    StructField("other_provider_name_suffix", StringType(), True),
    StructField("other_provider_entity_type", StringType(), True),
    StructField("other_provider_entity_identifier_code", StringType(), True),
    StructField("other_provider_entity_type_qualifier", StringType(), True),
    StructField("other_provider_taxonomy_code", StringType(), True),
    StructField("other_provider_code", StringType(), True),
    StructField("other_provider_specialty_code", StringType(), True),
    StructField("other_provider_tax_id", StringType(), True),
    StructField("other_provider_tax_id_qualifier", StringType(), True),
    StructField("other_provider_state_license_number", StringType(), True),
    StructField("other_provider_upin", StringType(), True),
    StructField("other_provider_commercial_number", StringType(), True),
    StructField("other_provider_address_line1", StringType(), True),
    StructField("other_provider_address_line2", StringType(), True),
    StructField("other_provider_city", StringType(), True),
    StructField("other_provider_state", StringType(), True),
    StructField("other_provider_zip_code", StringType(), True),
    StructField("other_provider_country_code", StringType(), True),
    StructField("other_provider_phone_number", StringType(), True),
    StructField("other_provider_fax_number", StringType(), True),
    StructField("other_provider_email", StringType(), True),
    StructField("facility_provider_npi", StringType(), True),
    StructField("facility_provider_name", StringType(), True),
    StructField("facility_provider_first_name", StringType(), True),
    StructField("facility_provider_last_name", StringType(), True),
    StructField("facility_provider_middle_initial", StringType(), True),
    StructField("facility_provider_name_suffix", StringType(), True),
    StructField("facility_provider_entity_type", StringType(), True),
    StructField("facility_provider_entity_identifier_code", StringType(), True),
    StructField("facility_provider_entity_type_qualifier", StringType(), True),
    StructField("facility_provider_taxonomy_code", StringType(), True),
    StructField("facility_provider_code", StringType(), True),
    StructField("facility_provider_specialty_code", StringType(), True),
    StructField("facility_provider_tax_id", StringType(), True),
    StructField("facility_provider_tax_id_qualifier", StringType(), True),
    StructField("facility_provider_state_license_number", StringType(), True),
    StructField("facility_provider_upin", StringType(), True),
    StructField("facility_provider_commercial_number", StringType(), True),
    StructField("facility_provider_address_line1", StringType(), True),
    StructField("facility_provider_address_line2", StringType(), True),
    StructField("facility_provider_city", StringType(), True),
    StructField("facility_provider_state", StringType(), True),
    StructField("facility_provider_zip_code", StringType(), True),
    StructField("facility_provider_country_code", StringType(), True),
    StructField("facility_provider_phone_number", StringType(), True),
    StructField("facility_provider_fax_number", StringType(), True),
    StructField("facility_provider_email", StringType(), True),
    StructField("diagnosis_codes", ArrayType(StructType([
        StructField("healthcare_code_information", IntegerType(), True),
        StructField("code_list_qualifier_code", StringType(), True),
        StructField("industry_code", StringType(), True)
    ])), True),
	StructField("service_lines", ArrayType(StructType([
		StructField("service_line_num", StringType(), True),
		StructField("service_id_qualifier", StringType(), True),
		StructField("procedure_code", StringType(), True),
		StructField("charge_amount", FloatType(), True),
		StructField("service_units", FloatType(), True),
		StructField("unit_of_measure", StringType(), True),
		StructField("service_date_from", StringType(), True),
		StructField("service_date_to", StringType(), True),
		StructField("service_date_format", StringType(), True),
		StructField("service_time", StringType(), True),
		StructField("modifiers", ArrayType(StringType()), True),
		StructField("revenue_code", StringType(), True),
		StructField("composite_diagnosis_code_pointer", MapType(StringType(), StringType()), True)
	])), True)

])

def get_837p_schema():
    """Schema for 837P Professional claims"""
    return StructType([
    StructField("filename", StringType(), True),
    StructField("transaction_control_number", StringType(), True),
    StructField("claim_id", StringType(), True),
    StructField("total_claim_charge_amount", StringType(), True),
    StructField("claim_provider_supplier_signature_indicator", StringType(), True),
    StructField("claim_assignment_or_plan_participation_code", StringType(), True),
    StructField("claim_benefits_assignment_certification_indicator", StringType(), True),
    StructField("claim_release_of_information_code", StringType(), True),
    StructField("claim_reference_identification_qualifier", StringType(), True),
    StructField("claim_member_groupor_policyNumber", StringType(), True),
    StructField("claim_description", StringType(), True),
    StructField("claim_reference_identifier", StringType(), True),
    StructField("serviceline_reference_identification_qualifier", StringType(), True),
    StructField("serviceline_member_groupor_policyNumber", StringType(), True),
    StructField("serviceline_description", StringType(), True),
    StructField("serviceline_reference_identifier", StringType(), True),
    StructField("place_of_service_code", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("bht_transaction_set_purpose_code", StringType(), True),
    StructField("bht_submitter_transaction_identifier", StringType(), True),
    StructField("bht_transaction_set_creation_date", StringType(), True),
    StructField("bht_transaction_set_creation_time", StringType(), True),
    StructField("bht_transaction_type_code", StringType(), True),
    StructField("patient_hierarchical_id_number", StringType(), True),
    StructField("patient_hierarchical_parent_id_number", StringType(), True),
    StructField("patient_hierarchical_level_code", StringType(), True),
    StructField("patient_hierarchical_child_code", StringType(), True),
    StructField("patient_entity_identifier_code", StringType(), True),
    StructField("patient_entity_type_qualifier", StringType(), True),
    StructField("patient_first_name", StringType(), True),
    StructField("patient_last_name", StringType(), True),
    StructField("patient_middle_initial", StringType(), True),
    StructField("patient_name_suffix", StringType(), True),
    StructField("patient_identification_code_qualifier", StringType(), True),
    StructField("patient_response_contact_identifier", StringType(), True),
    StructField("patient_date_of_birth", StringType(), True),
    StructField("patient_dob_format", StringType(), True),
    StructField("patient_gender_code", StringType(), True),
    StructField("patient_marital_status", StringType(), True),
    StructField("patient_ssn", StringType(), True),
    StructField("patient_member_id", StringType(), True),
    StructField("patient_medical_record_number", StringType(), True),
    StructField("patient_account_number", StringType(), True),
    StructField("patient_address_line1", StringType(), True),
    StructField("patient_address_line2", StringType(), True),
    StructField("patient_city", StringType(), True),
    StructField("patient_state", StringType(), True),
    StructField("patient_zip_code", StringType(), True),
    StructField("patient_country_code", StringType(), True),
    StructField("patient_phone_number", StringType(), True),
    StructField("patient_fax_number", StringType(), True),
    StructField("patient_email", StringType(), True),
    StructField("patient_relationship_code", StringType(), True),
    StructField("patient_payer_responsibility_sequenceNumber_code", StringType(), True),
    StructField("patient_claim_filing_indicator_code", StringType(), True),
    StructField("patient_employment_status", StringType(), True),
    StructField("patient_student_status", StringType(), True),
    StructField("patient_death_date", StringType(), True),
    StructField("subscriber_hierarchical_id_number", StringType(), True),
    StructField("subscriber_hierarchical_parent_id_number", StringType(), True),
    StructField("subscriber_hierarchical_level_code", StringType(), True),
    StructField("subscriber_hierarchical_child_code", StringType(), True),
    StructField("subscriber_entity_identifier_code", StringType(), True),
    StructField("subscriber_entity_type_qualifier", StringType(), True),
    StructField("subscriber_first_name", StringType(), True),
    StructField("subscriber_last_name", StringType(), True),
    StructField("subscriber_middle_initial", StringType(), True),
    StructField("subscriber_name_suffix", StringType(), True),
    StructField("subscriber_identification_code_qualifier", StringType(), True),
    StructField("subscriber_response_contact_identifier", StringType(), True),
    StructField("subscriber_date_of_birth", StringType(), True),
    StructField("subscriber_dob_format", StringType(), True),
    StructField("subscriber_gender_code", StringType(), True),
    StructField("subscriber_marital_status", StringType(), True),
    StructField("subscriber_ssn", StringType(), True),
    StructField("subscriber_member_id", StringType(), True),
    StructField("subscriber_medical_record_number", StringType(), True),
    StructField("subscriber_account_number", StringType(), True),
    StructField("subscriber_address_line1", StringType(), True),
    StructField("subscriber_address_line2", StringType(), True),
    StructField("subscriber_city", StringType(), True),
    StructField("subscriber_state", StringType(), True),
    StructField("subscriber_zip_code", StringType(), True),
    StructField("subscriber_country_code", StringType(), True),
    StructField("subscriber_phone_number", StringType(), True),
    StructField("subscriber_fax_number", StringType(), True),
    StructField("subscriber_email", StringType(), True),
    StructField("subscriber_relationship_code", StringType(), True),
    StructField("subscriber_payer_responsibility_sequenceNumber_code", StringType(), True),
    StructField("subscriber_claim_filing_indicator_code", StringType(), True),
    StructField("subscriber_employment_status", StringType(), True),
    StructField("subscriber_student_status", StringType(), True),
    StructField("subscriber_death_date", StringType(), True),
    StructField("payer_name", StringType(), True),
    StructField("payer_id", StringType(), True),
    StructField("payer_id_qualifier", StringType(), True),
    StructField("payer_entity_type", StringType(), True),
    StructField("payer_entity_code", StringType(), True),
    StructField("payer_address_line1", StringType(), True),
    StructField("payer_address_line2", StringType(), True),
    StructField("payer_city", StringType(), True),
    StructField("payer_state", StringType(), True),
    StructField("payer_zip_code", StringType(), True),
    StructField("payer_country_code", StringType(), True),
    StructField("payer_phone_number", StringType(), True),
    StructField("payer_fax_number", StringType(), True),
    StructField("submitter_name", StringType(), True),
    StructField("submitter_entity_type", StringType(), True),
    StructField("submitter_entity_type_qualifier", StringType(), True),
    StructField("submitter_entity_identifier_code", StringType(), True),
    StructField("submitter_id", StringType(), True),
    StructField("submitter_id_qualifier", StringType(), True),
    StructField("submitter_contact_function_code", StringType(), True),
    StructField("submitter_response_contact_name", StringType(), True),
    StructField("submitter_communication_qualifier", StringType(), True),
    StructField("submitter_communication_qualifier_number", StringType(), True),
    StructField("receiver_name", StringType(), True),
    StructField("receiver_entity_type", StringType(), True),
    StructField("receiver_entity_type_qualifier", StringType(), True),
    StructField("receiver_entity_identifier_code", StringType(), True),
    StructField("receiver_id", StringType(), True),
    StructField("receiver_id_qualifier", StringType(), True),
    StructField("receiver_contact_function_code", StringType(), True),
    StructField("receiver_response_contact_name", StringType(), True),
    StructField("receiver_communication_qualifier", StringType(), True),
    StructField("receiver_communication_qualifier_number", StringType(), True),
    StructField("billing_hierarchical_id_number", StringType(), True),
    StructField("billing_hierarchical_parent_id_number", StringType(), True),
    StructField("billing_hierarchical_level_code", StringType(), True),
    StructField("billing_hierarchical_child_code", StringType(), True),
    StructField("billing_provider_npi", StringType(), True),
    StructField("billing_provider_name", StringType(), True),
    StructField("billing_provider_first_name", StringType(), True),
    StructField("billing_provider_last_name", StringType(), True),
    StructField("billing_provider_middle_initial", StringType(), True),
    StructField("billing_provider_name_suffix", StringType(), True),
    StructField("billing_provider_entity_type", StringType(), True),
    StructField("billing_provider_entity_identifier_code", StringType(), True),
    StructField("billing_provider_entity_type_qualifier", StringType(), True),
    StructField("billing_provider_taxonomy_code", StringType(), True),
    StructField("billing_provider_code", StringType(), True),
    StructField("billing_provider_specialty_code", StringType(), True),
    StructField("billing_provider_tax_id", StringType(), True),
    StructField("billing_provider_tax_id_qualifier", StringType(), True),
    StructField("billing_provider_state_license_number", StringType(), True),
    StructField("billing_provider_upin", StringType(), True),
    StructField("billing_provider_commercial_number", StringType(), True),
    StructField("billing_provider_address_line1", StringType(), True),
    StructField("billing_provider_address_line2", StringType(), True),
    StructField("billing_provider_city", StringType(), True),
    StructField("billing_provider_state", StringType(), True),
    StructField("billing_provider_zip_code", StringType(), True),
    StructField("billing_provider_country_code", StringType(), True),
    StructField("billing_provider_phone_number", StringType(), True),
    StructField("billing_provider_fax_number", StringType(), True),
    StructField("billing_provider_email", StringType(), True),
    StructField("referring_provider_npi", StringType(), True),
    StructField("referring_provider_name", StringType(), True),
    StructField("referring_provider_first_name", StringType(), True),
    StructField("referring_provider_last_name", StringType(), True),
    StructField("referring_provider_middle_initial", StringType(), True),
    StructField("referring_provider_name_suffix", StringType(), True),
    StructField("referring_provider_entity_type", StringType(), True),
    StructField("referring_provider_entity_identifier_code", StringType(), True),
    StructField("referring_provider_entity_type_qualifier", StringType(), True),
    StructField("referring_provider_taxonomy_code", StringType(), True),
    StructField("referring_provider_code", StringType(), True),
    StructField("referring_provider_specialty_code", StringType(), True),
    StructField("referring_provider_tax_id", StringType(), True),
    StructField("referring_provider_tax_id_qualifier", StringType(), True),
    StructField("referring_provider_state_license_number", StringType(), True),
    StructField("referring_provider_upin", StringType(), True),
    StructField("referring_provider_commercial_number", StringType(), True),
    StructField("referring_provider_address_line1", StringType(), True),
    StructField("referring_provider_address_line2", StringType(), True),
    StructField("referring_provider_city", StringType(), True),
    StructField("referring_provider_state", StringType(), True),
    StructField("referring_provider_zip_code", StringType(), True),
    StructField("referring_provider_country_code", StringType(), True),
    StructField("referring_provider_phone_number", StringType(), True),
    StructField("referring_provider_fax_number", StringType(), True),
    StructField("referring_provider_email", StringType(), True),
    StructField("rendering_provider_npi", StringType(), True),
    StructField("rendering_provider_name", StringType(), True),
    StructField("rendering_provider_first_name", StringType(), True),
    StructField("rendering_provider_last_name", StringType(), True),
    StructField("rendering_provider_middle_initial", StringType(), True),
    StructField("rendering_provider_name_suffix", StringType(), True),
    StructField("rendering_provider_entity_type", StringType(), True),
    StructField("rendering_provider_entity_identifier_code", StringType(), True),
    StructField("rendering_provider_entity_type_qualifier", StringType(), True),
    StructField("rendering_provider_taxonomy_code", StringType(), True),
    StructField("rendering_provider_code", StringType(), True),
    StructField("rendering_provider_specialty_code", StringType(), True),
    StructField("rendering_provider_tax_id", StringType(), True),
    StructField("rendering_provider_tax_id_qualifier", StringType(), True),
    StructField("rendering_provider_state_license_number", StringType(), True),
    StructField("rendering_provider_upin", StringType(), True),
    StructField("rendering_provider_commercial_number", StringType(), True),
    StructField("rendering_provider_address_line1", StringType(), True),
    StructField("rendering_provider_address_line2", StringType(), True),
    StructField("rendering_provider_city", StringType(), True),
    StructField("rendering_provider_state", StringType(), True),
    StructField("rendering_provider_zip_code", StringType(), True),
    StructField("rendering_provider_country_code", StringType(), True),
    StructField("rendering_provider_phone_number", StringType(), True),
    StructField("rendering_provider_fax_number", StringType(), True),
    StructField("rendering_provider_email", StringType(), True),
    StructField("service_facility_provider_npi", StringType(), True),
    StructField("service_facility_provider_name", StringType(), True),
    StructField("service_facility_provider_first_name", StringType(), True),
    StructField("service_facility_provider_last_name", StringType(), True),
    StructField("service_facility_provider_middle_initial", StringType(), True),
    StructField("service_facility_provider_name_suffix", StringType(), True),
    StructField("service_facility_provider_entity_type", StringType(), True),
    StructField("service_facility_provider_entity_identifier_code", StringType(), True),
    StructField("service_facility_provider_entity_type_qualifier", StringType(), True),
    StructField("service_facility_provider_taxonomy_code", StringType(), True),
    StructField("service_facility_provider_code", StringType(), True),
    StructField("service_facility_provider_specialty_code", StringType(), True),
    StructField("service_facility_provider_tax_id", StringType(), True),
    StructField("service_facility_provider_tax_id_qualifier", StringType(), True),
    StructField("service_facility_provider_state_license_number", StringType(), True),
    StructField("service_facility_provider_upin", StringType(), True),
    StructField("service_facility_provider_commercial_number", StringType(), True),
    StructField("service_facility_provider_address_line1", StringType(), True),
    StructField("service_facility_provider_address_line2", StringType(), True),
    StructField("service_facility_provider_city", StringType(), True),
    StructField("service_facility_provider_state", StringType(), True),
    StructField("service_facility_provider_zip_code", StringType(), True),
    StructField("service_facility_provider_country_code", StringType(), True),
    StructField("service_facility_provider_phone_number", StringType(), True),
    StructField("service_facility_provider_fax_number", StringType(), True),
    StructField("service_facility_provider_email", StringType(), True),
    StructField("diagnosis_codes", ArrayType(StructType([
        StructField("healthcare_code_information", IntegerType(), True),
        StructField("code_list_qualifier_code", StringType(), True),
        StructField("industry_code", StringType(), True)
    ])), True),
    StructField("service_lines", ArrayType(StructType([
        StructField("service_line_num", StringType(), True),
        StructField("service_id_qualifier", StringType(), True),
        StructField("procedure_code", StringType(), True),
        StructField("charge_amount", FloatType(), True),
        StructField("service_units", FloatType(), True),
        StructField("unit_of_measure", StringType(), True),
        StructField("service_date_from", StringType(), True),
        StructField("service_date_to", StringType(), True),
        StructField("service_date_format", StringType(), True),
        StructField("service_time", StringType(), True),
        StructField("modifiers", ArrayType(StringType()), True),
        StructField("place_of_service_code", StringType(), True),
        StructField("composite_diagnosis_code_pointer", StructType([
            StructField("diagnosis_code_pointer_1", StringType(), True)
        ]), True)
    ])), True)
])


# COMMAND ----------

# DBTITLE 1,Data Quality Validation Functions
def validate_missing_elements_837i(df):
    """Validate and report on missing elements for 837I"""
    print("=== 837I Missing Element Report ===")
    
    key_fields = ['claim_id', 'patient_first_name', 'patient_last_name', 'total_claim_charge_amount', 'billing_provider_name']
    
    for field in key_fields:
        if field in df.columns:
            null_count = df.filter(col(field).isNull() | (col(field) == "")).count()
            total_count = df.count()
            completeness = ((total_count - null_count) / total_count) * 100 if total_count > 0 else 0
            print(f"{field}: {completeness:.1f}% complete ({null_count} nulls)")
    
    # Check array field completeness
    array_fields = ['diagnosis_codes', 'service_lines']
    for field in array_fields:
        if field in df.columns:
            empty_count = df.filter(
                col(field).isNull() | (size(col(field)) == 0)
            ).count()
            total_count = df.count()
            completeness = ((total_count - empty_count) / total_count) * 100 if total_count > 0 else 0
            print(f"{field}: {completeness:.1f}% have data ({empty_count} empty)")

def validate_missing_elements_837p(df):
    """Validate and report on missing elements for 837P"""
    print("=== 837P Missing Element Report ===")
    
    key_fields = ['claim_id', 'patient_first_name', 'patient_last_name', 'total_claim_charge_amount', 'rendering_provider_name']
    
    for field in key_fields:
        if field in df.columns:
            null_count = df.filter(col(field).isNull() | (col(field) == "")).count()
            total_count = df.count()
            completeness = ((total_count - null_count) / total_count) * 100 if total_count > 0 else 0
            print(f"{field}: {completeness:.1f}% complete ({null_count} nulls)")


# COMMAND ----------

# DBTITLE 1,Enhanced Processing Pipeline
def process_claims_with_denormalized_schema():
    """Optimized claim processing pipeline with denormalized schema"""
    
    # Step 1: Read sample data (both 837I and 837P)
    hm = HealthcareManager()
    # df = spark.read.text("file:///" + os.getcwd() + "/../sampledata/837/*test.txt", wholetext=True)
    
    path_837i = "dbfs:/mnt/data/sdsclaims/I_2507660392_UF20250317056001_077012829888.txt"
    path_837p = "dbfs:/mnt/data/sdsclaims/P_2507660518_CF20250317210044_077012805999.txt"
    path_837p_S = "dbfs:/mnt/data/sdsclaims/P_EDHC_CE827173587396_235098719609_999.txt"

    edi_files = "dbfs:/mnt/data/sdsclaims/edi/CC*.txt"
    sds_files = "dbfs:/mnt/data/sdsclaims/sds/*.837"


    df = spark.read.text(path_837p_S, wholetext = True)
    # Step 2: Calculate optimal partitions
    num_files = df.count()
    print(f"num_files {num_files}")
    optimal_partitions = min(spark.sparkContext.defaultParallelism * 2, max(num_files * 4, 4))
    print(f"Using {optimal_partitions} partitions for processing")
    
    # Step 3: Process with enhanced flattening
    rdd = (
        df.withColumn("filename", input_file_name()).rdd
        .map(lambda row: (row.filename, EDI(row.value)))
        .map(lambda edi: hm.flatten(edi[1], filename=edi[0]))
        .flatMap(lambda x: x)
    )
    
    print(f"Processed {rdd.count()} claim records")
    
    # Check if RDD is empty
    # print(f"Is RDD empty? {rdd.collect()}")

    # Step 4: Generate denormalized JSON
    claims_rdd = (
        rdd.map(lambda x: hm.flatten_to_denormalized_json(x))
        .map(lambda x: json.dumps(x))
    )
    
    # Step 5: Separate by transaction type and apply schemas
    claims_837i_rdd = claims_rdd.filter(lambda x: '"transaction_type": "223"' in x)
    claims_837p_rdd = claims_rdd.filter(lambda x: '"transaction_type": "222"' in x)

    # save the RDDs to JSON files
    dbutils.fs.rm("dbfs:/mnt/data/sdsclaims/output", True)
    claims_837p_rdd.map(lambda x: json.dumps(json.loads(x), indent=2)).coalesce(1).saveAsTextFile("dbfs:/mnt/data/sdsclaims/output/claims_837p")
    claims_837i_rdd.map(lambda x: json.dumps(json.loads(x), indent=2)).coalesce(1).saveAsTextFile("dbfs:/mnt/data/sdsclaims/output/claims_837i")
    
    # Step 6: Read with explicit schemas
    claims_837i = None
    claims_837p = None
    
    try:
        if claims_837i_rdd.isEmpty() == False:
            claims_837i = spark.read.schema(get_837i_schema()).json(claims_837i_rdd)
            print(f"837I Claims: {claims_837i.count()} records")
    except Exception as e:
        print(f"No 837I claims found or error processing: {e}")
    
    try:
        if claims_837p_rdd.isEmpty() == False:
            claims_837p = spark.read.schema(get_837p_schema()).json(claims_837p_rdd)
            print(f"837P Claims: {claims_837p.count()} records")
    except Exception as e:
        print(f"No 837P claims found or error processing: {e}")
    
    return claims_837i, claims_837p

# COMMAND ----------

# DBTITLE 1,Process Claims
claims_837i, claims_837p = process_claims_with_denormalized_schema()

# COMMAND ----------

# DBTITLE 1,Debugging
# MAGIC %debug
# COMMAND ----------

# DBTITLE 1,Analyze 837I Claims
if claims_837i is not None:
    print("=== 837I Institutional Claims Analysis ===")
    claims_837i.printSchema()
    
    print(f"Total 837I claims: {claims_837i.count()}")
    
    # Show sample data
    print("\n=== Sample 837I Data ===")
    claims_837i.select(
        "claim_id", 
        "patient_first_name", 
        "patient_last_name",
        "total_claim_charge_amount",
        "billing_provider_name",
        "transaction_type"
    ).show(5, truncate=False)
    
    # Analyze diagnosis codes
    print("\n=== Diagnosis Codes Analysis ===")
    claims_837i.select("claim_id", "diagnosis_codes").show(2, truncate=False)
    
    # Analyze service lines
    print("\n=== Service Lines Analysis ===")
    claims_837i.select("claim_id", "service_lines").show(2, truncate=False)
    
    # Data quality report
    print("\n=== 837I Data Quality Report ===")
    validate_missing_elements_837i(claims_837i)
else:
    print("No 837I claims found in the sample data")

# COMMAND ----------

# DBTITLE 1,Analyze 837P Claims
if claims_837p is not None:
    print("=== 837P Professional Claims Analysis ===")
    claims_837p.printSchema()
    
    print(f"Total 837P claims: {claims_837p.count()}")
    
    # Show sample data
    print("\n=== Sample 837P Data ===")
    claims_837p.select(
        "claim_id", 
        "patient_first_name", 
        "patient_last_name",
        "total_claim_charge_amount",
        "rendering_provider_name",
        "place_of_service_code"
    ).show(5, truncate=False)
    
    # Analyze diagnosis codes
    print("\n=== Diagnosis Codes Analysis ===")
    claims_837p.select("claim_id", "diagnosis_codes").show(2, truncate=False)
    
    # Analyze service lines
    print("\n=== Service Lines Analysis ===")
    claims_837p.select("claim_id", "service_lines").show(2, truncate=False)
    
    # Data quality report
    print("\n=== 837P Data Quality Report ===")
    validate_missing_elements_837p(claims_837p)
else:
    print("No 837P claims found in the sample data")


# COMMAND ----------

# DBTITLE 1,Save Denormalized Tables
def save_denormalized_tables():
    """Save denormalized tables with optimizations"""
    
    if claims_837i is not None:
        print("Saving 837I denormalized table...")
        (claims_837i
         .coalesce(2)
         .write
         .mode("overwrite")
         .option("compression", "snappy")
         .option("maxRecordsPerFile", 10000)
         .saveAsTable("claims_837i_denormalized"))
        print("837I table saved successfully")
    
    if claims_837p is not None:
        print("Saving 837P denormalized table...")
        (claims_837p
         .coalesce(2)
         .write
         .mode("overwrite")
         .option("compression", "snappy")
         .option("maxRecordsPerFile", 10000)
         .saveAsTable("claims_837p_denormalized"))
        print("837P table saved successfully")

# COMMAND ----------

# DBTITLE 1,Save Tables
save_denormalized_tables()

# COMMAND ----------

# DBTITLE 1,Query Examples
# MAGIC %sql
# MAGIC -- Query denormalized 837I claims
# MAGIC SELECT 
# MAGIC   claim_id,
# MAGIC   patient_first_name,
# MAGIC   patient_last_name,
# MAGIC   patient_state,
# MAGIC   total_claim_charge_amount,
# MAGIC   billing_provider_name,
# MAGIC   size(diagnosis_codes) as num_diagnoses,
# MAGIC   size(service_lines) as num_service_lines
# MAGIC FROM claims_837i_denormalized
# MAGIC WHERE patient_state IS NOT NULL
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query denormalized 837P claims  
# MAGIC SELECT 
# MAGIC   claim_id,
# MAGIC   patient_first_name,
# MAGIC   patient_last_name,
# MAGIC   total_claim_charge_amount,
# MAGIC   rendering_provider_name,
# MAGIC   size(diagnosis_codes) as num_diagnoses,
# MAGIC   size(service_lines) as num_service_lines
# MAGIC FROM claims_837p_denormalized
# MAGIC WHERE rendering_provider_name IS NOT NULL
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Explode diagnosis codes for analysis
# MAGIC SELECT 
# MAGIC   claim_id,
# MAGIC   diagnosis.seq,
# MAGIC   diagnosis.code,
# MAGIC   diagnosis.type
# MAGIC FROM claims_837i_denormalized
# MAGIC LATERAL VIEW explode(diagnosis_codes) t AS diagnosis
# MAGIC WHERE diagnosis_codes IS NOT NULL
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC This enhanced notebook demonstrates:
# MAGIC 
# MAGIC 1. **Complete Denormalization**: Single tables for 837I and 837P with all demographic and provider data flattened
# MAGIC 2. **JSON Arrays**: Repeating elements (diagnosis codes, service lines) stored as structured arrays with sequence numbers
# MAGIC 3. **Missing Element Handling**: Graceful handling of optional EDI segments with nullable schema fields
# MAGIC 4. **Performance Optimization**: Explicit schemas, optimal partitioning, and caching strategies
# MAGIC 5. **Data Quality Validation**: Comprehensive reporting on field completeness
# MAGIC 6. **Query Flexibility**: Examples of querying both flattened fields and JSON arrays
# MAGIC 
# MAGIC The result is a comprehensive, queryable, and performant denormalized schema that captures all available EDI data while maintaining healthcare industry standards.