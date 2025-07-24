-- models/intermediate/int_ad_value_propositions.sql
-- This model normalizes JSON value propositions from ad creative analysis into structured rows

{{ config(
    materialized='table',
    indexes=[
        {'columns': ['ad_account_id'], 'type': 'btree'},
        {'columns': ['value_proposition'], 'type': 'btree'},
        {'columns': ['ad_analysis_id'], 'type': 'btree'}
    ]
) }}

with ad_value_props as (
    select
        ca.cad_creative_analysis_id,
        ca.ad_analysis_id,
        aa.ads_meta_id,
        aa.ad_creative_id,
        ca.value_propositions,
        ca.loaded_at
    from {{ ref('stg_cad__creative_analysis') }} ca
    join {{ ref('stg_cad__ads_analysis') }} aa on ca.ad_analysis_id = aa.ad_analysis_id
    where ca.value_propositions is not null
),

-- Get ad account ID through the ads hierarchy
ad_with_account as (
    select
        avp.*,
        a.ad_account_id
    from ad_value_props avp
    join {{ ref('stg_cad__ads_meta') }} am on avp.ads_meta_id = am.ads_meta_id
    join {{ ref('stg_cad__ads') }} a on am.ad_id = a.ad_id
),

-- Extract value propositions from JSON array
normalized_value_props as (
    select
        cad_creative_analysis_id,
        ad_analysis_id,
        ads_meta_id,
        ad_creative_id,
        ad_account_id,
        trim(jsonb_extract_path_text(value_prop.value, 'proposition')) as value_proposition,
        trim(jsonb_extract_path_text(value_prop.value, 'strength')) as strength_score,
        trim(jsonb_extract_path_text(value_prop.value, 'clarity')) as clarity_score,
        loaded_at
    from ad_with_account awa
    cross join jsonb_array_elements(
        case 
            when awa.value_propositions is null then '[]'::jsonb
            when jsonb_typeof(awa.value_propositions) = 'array' then awa.value_propositions
            when jsonb_typeof(awa.value_propositions) = 'string' then 
                case 
                    when trim(awa.value_propositions::text, '"') ~ '^\[.*\]$' then trim(awa.value_propositions::text, '"')::jsonb
                    else '[]'::jsonb
                end
            else '[]'::jsonb
        end
    ) as value_prop
    where jsonb_extract_path_text(value_prop.value, 'proposition') is not null 
      and trim(jsonb_extract_path_text(value_prop.value, 'proposition')) != ''
),

-- Clean and standardize value propositions
final as (
    select
        cad_creative_analysis_id,
        ad_analysis_id,
        ads_meta_id,
        ad_creative_id,
        ad_account_id,
        lower(trim(value_proposition)) as value_proposition,
        case 
            when strength_score ~ '^[0-9]+\.?[0-9]*$' 
            then strength_score::numeric 
            else null 
        end as strength_score,
        case 
            when clarity_score ~ '^[0-9]+\.?[0-9]*$' 
            then clarity_score::numeric 
            else null 
        end as clarity_score,
        'cad' as source_system,
        loaded_at
    from normalized_value_props
    where value_proposition is not null
      and value_proposition != ''
      and value_proposition != 'null'
)

select * from final 