-- models/intermediate/int_persona_gap_analysis_simple.sql
-- Simplified gap analysis model that works without cross-account dependencies

{{ config(
    materialized='table',
    indexes=[
        {'columns': ['ad_account_id'], 'type': 'btree'},
        {'columns': ['persona_attribute'], 'type': 'btree'},
        {'columns': ['gap_priority'], 'type': 'btree'}
    ]
) }}

with review_personas as (
    select
        ad_account_id,
        persona_attribute,
        count(distinct review_id) as review_count,
        count(*) as total_mentions,
        avg(confidence_score) as avg_confidence,
        max(review_date) as latest_review_date,
        min(review_date) as earliest_review_date
    from {{ ref('int_review_persona_attributes') }}
    group by 1, 2
),

ad_personas as (
    select distinct
        ad_account_id,
        persona_attribute,
        count(*) as ad_mention_count
    from {{ ref('int_ad_persona_attributes') }}
    group by 1, 2
),

gap_analysis as (
    select
        coalesce(rp.ad_account_id, ap.ad_account_id) as ad_account_id,
        coalesce(rp.persona_attribute, ap.persona_attribute) as persona_attribute,
        
        -- Review metrics
        coalesce(rp.review_count, 0) as review_count,
        coalesce(rp.total_mentions, 0) as total_mentions,
        rp.avg_confidence,
        rp.latest_review_date,
        rp.earliest_review_date,
        
        -- Ad metrics  
        coalesce(ap.ad_mention_count, 0) as ad_mention_count,
        
        -- Gap analysis
        case 
            when rp.persona_attribute is not null and ap.persona_attribute is null then 'completely_unaddressed'
            when rp.persona_attribute is not null and ap.persona_attribute is not null then 'has_coverage'
            when rp.persona_attribute is null and ap.persona_attribute is not null then 'ads_without_reviews'
            else 'unknown'
        end as gap_type,
        
        -- Priority scoring
        case 
            when rp.persona_attribute is not null and ap.persona_attribute is null then rp.review_count * 20
            when rp.persona_attribute is not null and ap.persona_attribute is not null then rp.review_count * 5
            else 0
        end as priority_score,
        
        current_timestamp as loaded_at
        
    from review_personas rp
    full outer join ad_personas ap on rp.ad_account_id = ap.ad_account_id 
                                   and rp.persona_attribute = ap.persona_attribute
),

final as (
    select
        ga.*,
        aa.ad_account_name,
        
        -- Gap priority
        case 
            when ga.gap_type = 'completely_unaddressed' and ga.review_count >= 5 then 'critical'
            when ga.gap_type = 'completely_unaddressed' and ga.review_count >= 2 then 'high'
            when ga.gap_type = 'has_coverage' and ga.review_count >= 10 then 'medium'
            else 'low'
        end as gap_priority,
        
        -- Recommendations
        case 
            when ga.gap_type = 'completely_unaddressed' then 'Create ad campaigns targeting this persona'
            when ga.gap_type = 'has_coverage' then 'Review current ad performance for this persona'
            when ga.gap_type = 'ads_without_reviews' then 'Validate if this persona resonates with customers'
            else 'Monitor'
        end as recommendation
        
    from gap_analysis ga
    join {{ ref('stg_cad__ad_accounts') }} aa on ga.ad_account_id = aa.ad_account_id
    where ga.gap_type in ('completely_unaddressed', 'has_coverage')
      and ga.review_count >= 1
)

select * from final
order by priority_score desc, review_count desc 