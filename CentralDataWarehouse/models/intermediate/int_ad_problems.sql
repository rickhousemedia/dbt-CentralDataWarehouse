-- models/intermediate/int_ad_problems.sql
-- This model normalizes JSON problems from ad creative analysis into structured rows

{{ config(
    materialized='table',
    indexes=[
        {'columns': ['ad_account_id'], 'type': 'btree'},
        {'columns': ['problem'], 'type': 'btree'},
        {'columns': ['ad_analysis_id'], 'type': 'btree'}
    ]
) }}

with ad_problems as (
    select
        ca.cad_creative_analysis_id,
        ca.ad_analysis_id,
        aa.ads_meta_id,
        aa.ad_creative_id,
        ca.problems,
        ca.loaded_at
    from {{ ref('stg_cad__creative_analysis') }} ca
    join {{ ref('stg_cad__ads_analysis') }} aa on ca.ad_analysis_id = aa.ad_analysis_id
    where ca.problems is not null
),

-- Get ad account ID through the ads hierarchy
ad_with_account as (
    select
        ap.*,
        a.ad_account_id
    from ad_problems ap
    join {{ ref('stg_cad__ads_meta') }} am on ap.ads_meta_id = am.ads_meta_id
    join {{ ref('stg_cad__ads') }} a on am.ad_id = a.ad_id
),

-- Extract problems from JSON array
normalized_problems as (
    select
        cad_creative_analysis_id,
        ad_analysis_id,
        ads_meta_id,
        ad_creative_id,
        ad_account_id,
        trim(jsonb_extract_path_text(problem.value, 'problem')) as problem,
        trim(jsonb_extract_path_text(problem.value, 'relevance')) as relevance_score,
        trim(jsonb_extract_path_text(problem.value, 'urgency')) as urgency_score,
        loaded_at
    from ad_with_account awa
    cross join jsonb_array_elements(
        case 
            when awa.problems is null then '[]'::jsonb
            when jsonb_typeof(awa.problems) = 'array' then awa.problems
            when jsonb_typeof(awa.problems) = 'string' then 
                case 
                    when trim(awa.problems::text, '"') ~ '^\[.*\]$' then trim(awa.problems::text, '"')::jsonb
                    else '[]'::jsonb
                end
            else '[]'::jsonb
        end
    ) as problem
    where jsonb_extract_path_text(problem.value, 'problem') is not null 
      and trim(jsonb_extract_path_text(problem.value, 'problem')) != ''
),

-- Clean and standardize problems
final as (
    select
        cad_creative_analysis_id,
        ad_analysis_id,
        ads_meta_id,
        ad_creative_id,
        ad_account_id,
        lower(trim(problem)) as problem,
        case 
            when relevance_score ~ '^[0-9]+\.?[0-9]*$' 
            then relevance_score::numeric 
            else null 
        end as relevance_score,
        case 
            when urgency_score ~ '^[0-9]+\.?[0-9]*$' 
            then urgency_score::numeric 
            else null 
        end as urgency_score,
        'cad' as source_system,
        loaded_at
    from normalized_problems
    where problem is not null
      and problem != ''
      and problem != 'null'
)

select * from final 