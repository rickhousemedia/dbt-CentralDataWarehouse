-- models/intermediate/int_ad_persona_attributes.sql
-- This model normalizes JSON persona attributes from ad creative analysis into structured rows

{{ config(
    materialized='table',
    indexes=[
        {'columns': ['ad_account_id'], 'type': 'btree'},
        {'columns': ['persona_attribute'], 'type': 'btree'},
        {'columns': ['ad_analysis_id'], 'type': 'btree'}
    ]
) }}

with ad_personas as (
    select
        ca.cad_creative_analysis_id,
        ca.ad_analysis_id,
        aa.ads_meta_id,
        aa.ad_creative_id,
        ca.persona_attributes,
        ca.core_persona,
        ca.loaded_at
    from {{ ref('stg_cad__creative_analysis') }} ca
    join {{ ref('stg_cad__ads_analysis') }} aa on ca.ad_analysis_id = aa.ad_analysis_id
    where ca.persona_attributes is not null
),

-- Get ad account ID through the ads hierarchy
ad_with_account as (
    select
        ap.*,
        a.ad_account_id
    from ad_personas ap
    join {{ ref('stg_cad__ads_meta') }} am on ap.ads_meta_id = am.ads_meta_id
    join {{ ref('stg_cad__ads') }} a on am.ad_id = a.ad_id
),

-- Extract persona attributes from JSON array (handle various data formats)
normalized_personas as (
    select
        cad_creative_analysis_id,
        ad_analysis_id,
        ads_meta_id,
        ad_creative_id,
        ad_account_id,
        core_persona,
        trim(jsonb_extract_path_text(persona_attr.value, 'attribute')) as persona_attribute,
        trim(jsonb_extract_path_text(persona_attr.value, 'value')) as persona_value,
        trim(jsonb_extract_path_text(persona_attr.value, 'confidence')) as confidence_score,
        loaded_at
    from ad_with_account awa
    cross join jsonb_array_elements(
        case 
            when awa.persona_attributes is null then '[]'::jsonb
            when jsonb_typeof(awa.persona_attributes) = 'array' then awa.persona_attributes
            when jsonb_typeof(awa.persona_attributes) = 'string' then 
                case 
                    when trim(awa.persona_attributes::text, '"') ~ '^\[.*\]$' then trim(awa.persona_attributes::text, '"')::jsonb
                    else '[]'::jsonb
                end
            else '[]'::jsonb
        end
    ) as persona_attr
    where jsonb_extract_path_text(persona_attr.value, 'attribute') is not null 
      and trim(jsonb_extract_path_text(persona_attr.value, 'attribute')) != ''
),

-- Clean and standardize persona attributes
final as (
    select
        cad_creative_analysis_id,
        ad_analysis_id,
        ads_meta_id,
        ad_creative_id,
        ad_account_id,
        core_persona,
        lower(trim(persona_attribute)) as persona_attribute,
        persona_value,
        case 
            when confidence_score ~ '^[0-9]+\.?[0-9]*$' 
            then confidence_score::numeric 
            else null 
        end as confidence_score,
        'cad' as source_system,
        loaded_at
    from normalized_personas
    where persona_attribute is not null
      and persona_attribute != ''
      and persona_attribute != 'null'
)

select * from final 