-- models/intermediate/int_ad_persona_attributes_from_review_analysis.sql
-- Extract persona attributes from Review tool's analysis of CAD ads
-- This is where the actual ad persona data lives!

{{ config(
    materialized='table',
    indexes=[
        {'columns': ['ad_account_id', 'persona_attribute'], 'type': 'btree'}
    ]
) }}

with review_ad_analysis as (
    select
        raca.review_creative_analysis_id,
        raca.ad_account_id,
        raca.ad_id,
        raca.persona_attributes,
        raca.hook_persona_attributes,
        raca.value_propositions,
        raca.loaded_at
    from {{ ref('stg_review__ad_creative_analysis') }} raca
    where raca.persona_attributes is not null
),

-- Extract persona attributes from structured JSON objects
-- Based on sample: [{"start_timestamp"...} - this appears to be structured differently than reviews
normalized_personas as (
    select
        raa.review_creative_analysis_id,
        raa.ad_account_id,
        raa.ad_id,
        raa.loaded_at,
        -- Handle both structured objects and simple strings
        case 
            when jsonb_typeof(persona_attr.value) = 'object' then
                coalesce(
                    jsonb_extract_path_text(persona_attr.value, 'persona'),
                    jsonb_extract_path_text(persona_attr.value, 'attribute'),
                    jsonb_extract_path_text(persona_attr.value, 'name'),
                    persona_attr.value::text
                )
            else persona_attr.value::text
        end as persona_attribute,
        
        case 
            when jsonb_typeof(persona_attr.value) = 'object' then
                coalesce(
                    jsonb_extract_path_text(persona_attr.value, 'value'),
                    jsonb_extract_path_text(persona_attr.value, 'description'),
                    persona_attr.value::text
                )
            else persona_attr.value::text
        end as persona_value,
        
        case 
            when jsonb_typeof(persona_attr.value) = 'object' then
                coalesce(
                    jsonb_extract_path_text(persona_attr.value, 'confidence')::numeric,
                    0.8
                )
            else 0.8
        end as confidence_score
    from review_ad_analysis raa
    cross join jsonb_array_elements(
        case 
            when raa.persona_attributes is null then '[]'::jsonb
            when jsonb_typeof(raa.persona_attributes) = 'array' then raa.persona_attributes
            else '[]'::jsonb
        end
    ) as persona_attr
    where persona_attr.value is not null
      and trim(persona_attr.value::text, '"') != ''
      and trim(persona_attr.value::text, '"') != 'null'
),

-- Also extract from hook_persona_attributes (which appears to be string array format)
hook_personas as (
    select
        raa.review_creative_analysis_id,
        raa.ad_account_id,
        raa.ad_id,
        raa.loaded_at,
        hook_persona_text as persona_attribute,
        hook_persona_text as persona_value,
        0.8 as confidence_score
    from review_ad_analysis raa
    cross join jsonb_array_elements_text(
        case 
            when raa.hook_persona_attributes is null then '[]'::jsonb
            when jsonb_typeof(raa.hook_persona_attributes) = 'array' then raa.hook_persona_attributes
            else '[]'::jsonb
        end
    ) as hook_persona_text
    where hook_persona_text is not null 
      and trim(hook_persona_text) != ''
      and trim(hook_persona_text) != 'null'
)

-- Combine both sources and clean up
select distinct
    ad_account_id,
    review_creative_analysis_id,
    ad_id,
    lower(trim(persona_attribute)) as persona_attribute,
    persona_value,
    confidence_score,
    'review_analysis_of_ads' as source_system,
    loaded_at
from (
    select * from normalized_personas
    union all
    select * from hook_personas
) combined
where persona_attribute is not null 
  and trim(persona_attribute) != ''
order by ad_account_id, persona_attribute 