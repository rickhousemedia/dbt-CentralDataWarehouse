-- models/intermediate/int_ad_barriers.sql
-- This model normalizes JSON barriers from ad creative analysis into structured rows

{{ config(
    materialized='table',
    indexes=[
        {'columns': ['ad_account_id'], 'type': 'btree'},
        {'columns': ['barrier'], 'type': 'btree'},
        {'columns': ['ad_analysis_id'], 'type': 'btree'}
    ]
) }}

with ad_barriers as (
    select
        ca.cad_creative_analysis_id,
        ca.ad_analysis_id,
        aa.ads_meta_id,
        aa.ad_creative_id,
        ca.barriers,
        ca.loaded_at
    from {{ ref('stg_cad__creative_analysis') }} ca
    join {{ ref('stg_cad__ads_analysis') }} aa on ca.ad_analysis_id = aa.ad_analysis_id
    where ca.barriers is not null
),

-- Get ad account ID through the ads hierarchy
ad_with_account as (
    select
        ab.*,
        a.ad_account_id
    from ad_barriers ab
    join {{ ref('stg_cad__ads_meta') }} am on ab.ads_meta_id = am.ads_meta_id
    join {{ ref('stg_cad__ads') }} a on am.ad_id = a.ad_id
),

-- Extract barriers from JSON array
normalized_barriers as (
    select
        cad_creative_analysis_id,
        ad_analysis_id,
        ads_meta_id,
        ad_creative_id,
        ad_account_id,
        trim(jsonb_extract_path_text(barrier.value, 'barrier')) as barrier,
        trim(jsonb_extract_path_text(barrier.value, 'solution_strength')) as solution_strength,
        trim(jsonb_extract_path_text(barrier.value, 'addressability')) as addressability_score,
        loaded_at
    from ad_with_account awa
    cross join jsonb_array_elements(
        case 
            when awa.barriers is null then '[]'::jsonb
            when jsonb_typeof(awa.barriers) = 'array' then awa.barriers
            when jsonb_typeof(awa.barriers) = 'string' then 
                case 
                    when trim(awa.barriers::text, '"') ~ '^\[.*\]$' then trim(awa.barriers::text, '"')::jsonb
                    else '[]'::jsonb
                end
            else '[]'::jsonb
        end
    ) as barrier
    where jsonb_extract_path_text(barrier.value, 'barrier') is not null 
      and trim(jsonb_extract_path_text(barrier.value, 'barrier')) != ''
),

-- Clean and standardize barriers
final as (
    select
        cad_creative_analysis_id,
        ad_analysis_id,
        ads_meta_id,
        ad_creative_id,
        ad_account_id,
        lower(trim(barrier)) as barrier,
        case 
            when solution_strength ~ '^[0-9]+\.?[0-9]*$' 
            then solution_strength::numeric 
            else null 
        end as solution_strength,
        case 
            when addressability_score ~ '^[0-9]+\.?[0-9]*$' 
            then addressability_score::numeric 
            else null 
        end as addressability_score,
        'cad' as source_system,
        loaded_at
    from normalized_barriers
    where barrier is not null
      and barrier != ''
      and barrier != 'null'
)

select * from final 