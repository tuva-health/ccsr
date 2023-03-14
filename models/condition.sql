{{ config(materialized='ephemeral') }}
select
    encounter_id,
    claim_id,
    patient_id,
    condition_date,
    condition_type,
    code_type,
    code,
    description,
    diagnosis_rank,
    present_on_admit_code,
    present_on_admit_description,
    data_source
from tuva_claims_demo_full.core.condition