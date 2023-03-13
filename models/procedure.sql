select 
    encounter_id,
    patient_id,
    procedure_date,
    code_type,
    code,
    description,
    practitioner_npi,
    data_source
from tuva_claims_demo_full.core.procedure