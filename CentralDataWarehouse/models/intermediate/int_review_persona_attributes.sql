-- models/intermediate/int_review_persona_attributes.sql
-- This model normalizes JSON persona attributes from customer reviews into structured rows

{{ config(
    materialized='table',
    indexes=[
        {'columns': ['ad_account_id'], 'type': 'btree'},
        {'columns': ['persona_attribute'], 'type': 'btree'},
        {'columns': ['review_date'], 'type': 'btree'}
    ]
) }}

with review_personas as (
    select
        review_id,
        ad_account_id,
        review_date,
        review_author,
        r_persona_attributes,
        loaded_at
    from {{ ref('stg_review__reviews') }}
    where r_persona_attributes is not null
),

-- Extract persona attributes from JSON array
normalized_personas as (
    select
        review_id,
        ad_account_id,
        review_date,
        review_author,
        trim(json_extract_path_text(persona_attr.value, 'attribute')) as persona_attribute,
        trim(json_extract_path_text(persona_attr.value, 'value')) as persona_value,
        trim(json_extract_path_text(persona_attr.value, 'confidence')) as confidence_score,
        loaded_at
    from review_personas rp
    cross join json_array_elements(rp.r_persona_attributes) as persona_attr
    where trim(json_extract_path_text(persona_attr.value, 'attribute')) != ''
),

-- Clean and standardize persona attributes
final as (
    select
        review_id,
        ad_account_id,
        review_date,
        review_author,
        lower(trim(persona_attribute)) as persona_attribute,
        persona_value,
        case 
            when confidence_score ~ '^[0-9]+\.?[0-9]*$' 
            then confidence_score::numeric 
            else null 
        end as confidence_score,
        'review' as source_system,
        loaded_at
    from normalized_personas
    where persona_attribute is not null
      and persona_attribute != ''
      and persona_attribute != 'null'
)

select * from final 