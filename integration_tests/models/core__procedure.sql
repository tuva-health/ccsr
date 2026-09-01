select
    cast(null as {{ dbt.type_string() }}) as procedure_id
  , cast(null as {{ dbt.type_string() }}) as encounter_id
  , cast(null as {{ dbt.type_string() }}) as claim_id
  , cast(null as {{ dbt.type_string() }}) as person_id
  , cast(null as {{ dbt.type_string() }}) as normalized_code
  , cast(null as {{ dbt.type_string() }}) as code_system
  , cast(null as {{ dbt.type_string() }}) as data_source
where 1 = 0
