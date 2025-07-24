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

-- Extract persona attributes from JSON array (handling simple string format)
normalized_personas as (
    select
        review_id,
        ad_account_id,
        review_date,
        review_author,
        -- Parse the string value to extract attribute and value
        case 
            when persona_attr_text like '%:%' then trim(split_part(persona_attr_text, ':', 1))
            else persona_attr_text
        end as persona_attribute,
        case 
            when persona_attr_text like '%:%' then trim(split_part(persona_attr_text, ':', 2))
            else persona_attr_text
        end as persona_value,
        0.8 as confidence_score,  -- Default confidence since not provided in data
        loaded_at
    from review_personas rp
    cross join jsonb_array_elements_text(
        case 
            when rp.r_persona_attributes is null then '[]'::jsonb
            when jsonb_typeof(rp.r_persona_attributes) = 'array' then rp.r_persona_attributes
            else '[]'::jsonb
        end
    ) as persona_attr_text
    where persona_attr_text is not null 
      and trim(persona_attr_text) != ''
      and trim(persona_attr_text) != 'null'
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
        confidence_score as confidence_score,
        'review' as source_system,
        loaded_at
    from normalized_personas
    where persona_attribute is not null
      and persona_attribute != ''
      and persona_attribute != 'null'
)

select * from final 