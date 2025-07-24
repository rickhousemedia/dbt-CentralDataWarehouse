-- models/intermediate/int_unified_persona_attributes.sql
-- This model creates a unified view of persona attributes from both reviews and ads

{{ config(
    materialized='table',
    indexes=[
        {'columns': ['ad_account_id'], 'type': 'btree'},
        {'columns': ['persona_attribute'], 'type': 'btree'},
        {'columns': ['source_system'], 'type': 'btree'}
    ]
) }}

with review_personas as (
    select
        ad_account_id,
        persona_attribute,
        persona_value,
        confidence_score,
        source_system,
        count(*) as mention_count,
        count(distinct review_id) as review_count,
        avg(confidence_score) as avg_confidence_score,
        max(review_date) as latest_mention_date,
        min(review_date) as earliest_mention_date
    from {{ ref('int_review_persona_attributes') }}
    group by 1, 2, 3, 4, 5
),

ad_personas as (
    select
        ad_account_id,
        persona_attribute,
        persona_value,
        confidence_score,
        source_system,
        count(*) as mention_count,
        count(distinct ads_meta_id) as ad_count,
        avg(confidence_score) as avg_confidence_score,
        max(loaded_at) as latest_mention_date,
        min(loaded_at) as earliest_mention_date
    from {{ ref('int_ad_persona_attributes') }}
    group by 1, 2, 3, 4, 5
),

unified_personas as (
    select
        ad_account_id,
        persona_attribute,
        persona_value,
        confidence_score,
        source_system,
        mention_count,
        review_count as entity_count,
        avg_confidence_score,
        latest_mention_date,
        earliest_mention_date,
        review_count,
        null as ad_count
    from review_personas
    
    union all
    
    select
        ad_account_id,
        persona_attribute,
        persona_value,
        confidence_score,
        source_system,
        mention_count,
        ad_count as entity_count,
        avg_confidence_score,
        latest_mention_date,
        earliest_mention_date,
        null as review_count,
        ad_count
    from ad_personas
),

-- Create summary statistics per persona attribute
persona_summary as (
    select
        ad_account_id,
        persona_attribute,
        count(distinct case when source_system = 'review' then persona_value end) as review_variations,
        count(distinct case when source_system = 'cad' then persona_value end) as ad_variations,
        sum(case when source_system = 'review' then mention_count else 0 end) as total_review_mentions,
        sum(case when source_system = 'cad' then mention_count else 0 end) as total_ad_mentions,
        avg(case when source_system = 'review' then avg_confidence_score end) as avg_review_confidence,
        avg(case when source_system = 'cad' then avg_confidence_score end) as avg_ad_confidence,
        max(case when source_system = 'review' then latest_mention_date end) as latest_review_mention,
        max(case when source_system = 'cad' then latest_mention_date end) as latest_ad_mention,
        case 
            when sum(case when source_system = 'review' then mention_count else 0 end) > 0 
             and sum(case when source_system = 'cad' then mention_count else 0 end) > 0 
            then 'both'
            when sum(case when source_system = 'review' then mention_count else 0 end) > 0 
            then 'review_only'
            else 'ad_only'
        end as coverage_type
    from unified_personas
    group by 1, 2
),

final as (
    select
        up.*,
        ps.review_variations,
        ps.ad_variations,
        ps.total_review_mentions,
        ps.total_ad_mentions,
        ps.avg_review_confidence,
        ps.avg_ad_confidence,
        ps.latest_review_mention,
        ps.latest_ad_mention,
        ps.coverage_type,
        current_timestamp as loaded_at
    from unified_personas up
    join persona_summary ps on up.ad_account_id = ps.ad_account_id 
                             and up.persona_attribute = ps.persona_attribute
)

select * from final 