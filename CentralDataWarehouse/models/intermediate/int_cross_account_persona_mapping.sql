-- models/intermediate/int_cross_account_persona_mapping.sql
-- This model maps personas between CAD and Review accounts using the DWH mapping table

{{ config(
    materialized='table',
    indexes=[
        {'columns': ['cad_account_id'], 'type': 'btree'},
        {'columns': ['review_account_id'], 'type': 'btree'},
        {'columns': ['persona_attribute'], 'type': 'btree'}
    ]
) }}

with mapping_table as (
    select 
        ad_account_id as cad_account_id,
        ad_account_name as cad_account_name,
        product as cad_product,
        matched_review_product,
        review_account_name,
        review_account_id
    from {{ ref('CAD_Reviews_DWH_mapping_table') }}
    where review_account_id is not null  -- Only mapped accounts
),

-- Get personas from CAD ads with mapping context
cad_personas_mapped as (
    select
        apa.ad_account_id as cad_account_id,
        apa.persona_attribute,
        apa.persona_value,
        apa.confidence_score,
        apa.ads_meta_id,
        apa.core_persona,
        mt.cad_account_name,
        mt.review_account_id,
        mt.review_account_name,
        count(*) over (partition by apa.ad_account_id, apa.persona_attribute) as cad_persona_mentions,
        count(distinct apa.ads_meta_id) over (partition by apa.ad_account_id, apa.persona_attribute) as cad_ads_with_persona
    from {{ ref('int_ad_persona_attributes') }} apa
    join mapping_table mt on apa.ad_account_id = mt.cad_account_id
),

-- Get personas from Reviews with mapping context
review_personas_mapped as (
    select
        rpa.ad_account_id as original_review_account_id,
        rpa.persona_attribute,
        rpa.persona_value,
        rpa.confidence_score,
        rpa.review_id,
        rpa.review_date,
        -- Map to CAD account through the mapping table
        mt.cad_account_id,
        mt.cad_account_name,
        mt.review_account_name,
        count(*) over (partition by rpa.ad_account_id, rpa.persona_attribute) as review_persona_mentions,
        count(distinct rpa.review_id) over (partition by rpa.ad_account_id, rpa.persona_attribute) as review_count_with_persona
    from {{ ref('int_review_persona_attributes') }} rpa
    join mapping_table mt on rpa.ad_account_id = mt.review_account_id
),

-- Cross-account persona analysis
cross_account_personas as (
    select
        cp.cad_account_id,
        cp.cad_account_name,
        cp.review_account_id,
        cp.review_account_name,
        cp.persona_attribute,
        
        -- CAD side metrics
        cp.cad_persona_mentions,
        cp.cad_ads_with_persona,
        count(distinct cp.ads_meta_id) as total_cad_ads,
        avg(cp.confidence_score) as avg_cad_confidence,
        
        -- Review side metrics (if exists)
        coalesce(rp.review_persona_mentions, 0) as review_persona_mentions,
        coalesce(rp.review_count_with_persona, 0) as review_count_with_persona,
        rp.avg_review_confidence,
        max(rp.review_date) as latest_review_mention,
        
        -- Cross-system analysis
        case 
            when rp.persona_attribute is not null then 'found_in_both'
            else 'cad_only'
        end as persona_coverage,
        
        -- Coverage score (higher = better alignment)
        case 
            when rp.persona_attribute is not null 
            then (cp.cad_persona_mentions + coalesce(rp.review_persona_mentions, 0)) * 
                 (coalesce(cp.confidence_score, 0) + coalesce(rp.confidence_score, 0)) / 2
            else cp.cad_persona_mentions * coalesce(cp.confidence_score, 0) * 0.5
        end as cross_system_alignment_score
        
    from cad_personas_mapped cp
    left join (
        select 
            cad_account_id,
            persona_attribute,
            sum(review_persona_mentions) as review_persona_mentions,
            sum(review_count_with_persona) as review_count_with_persona,
            avg(confidence_score) as avg_review_confidence,
            max(review_date) as review_date
        from review_personas_mapped 
        group by 1, 2
    ) rp on cp.cad_account_id = rp.cad_account_id 
         and cp.persona_attribute = rp.persona_attribute
    group by 1, 2, 3, 4, 5, 6, 7, 9, 10, 11, 12, 13
),

-- Add personas that exist only in reviews for mapped accounts
review_only_personas as (
    select
        rp.cad_account_id,
        rp.cad_account_name,
        rp.original_review_account_id as review_account_id,
        rp.review_account_name,
        rp.persona_attribute,
        
        -- CAD side metrics (none for review-only)
        0 as cad_persona_mentions,
        0 as cad_ads_with_persona,
        0 as total_cad_ads,
        null as avg_cad_confidence,
        
        -- Review side metrics
        rp.review_persona_mentions,
        rp.review_count_with_persona,
        avg(rp.confidence_score) as avg_review_confidence,
        max(rp.review_date) as latest_review_mention,
        
        -- Cross-system analysis
        'review_only' as persona_coverage,
        
        -- Coverage score
        rp.review_persona_mentions * avg(rp.confidence_score) * 0.3 as cross_system_alignment_score
        
    from review_personas_mapped rp
    left join cad_personas_mapped cp on rp.cad_account_id = cp.cad_account_id 
                                      and rp.persona_attribute = cp.persona_attribute
    where cp.persona_attribute is null  -- Only personas not found in CAD
    group by 1, 2, 3, 4, 5, 10, 11
),

-- Combine all personas
final as (
    select * from cross_account_personas
    union all
    select * from review_only_personas
)

select 
    *,
    current_timestamp as loaded_at
from final
order by cross_system_alignment_score desc 