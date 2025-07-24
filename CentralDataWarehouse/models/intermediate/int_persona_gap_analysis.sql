-- models/intermediate/int_persona_gap_analysis.sql
-- This model identifies persona gaps - personas mentioned in reviews but not well-served by ads

{{ config(
    materialized='table',
    indexes=[
        {'columns': ['ad_account_id'], 'type': 'btree'},
        {'columns': ['gap_priority'], 'type': 'btree'},
        {'columns': ['opportunity_score'], 'type': 'btree'}
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

ad_performance_by_persona as (
    select
        ad_account_id,
        persona_attribute,
        performance_tier,
        gap_analysis_category,
        opportunity_score,
        ad_count,
        avg_roas,
        avg_ctr,
        total_spend,
        composite_performance_score,
        persona_coverage,
        cross_system_alignment_score,
        account_mapping_status
    from {{ ref('int_persona_performance_scorecard') }}
),

gap_analysis as (
    select
        rp.ad_account_id,
        rp.persona_attribute,
        rp.review_count,
        rp.total_mentions,
        rp.avg_confidence,
        rp.latest_review_date,
        rp.earliest_review_date,
        null as persona_coverage,
        0 as cross_system_alignment_score,
        null as review_account_name,
        null as review_account_id,
        'Unknown' as cad_account_name,
        
        -- Ad performance data (null if persona not in ads)
        coalesce(ap.ad_count, 0) as ad_count,
        ap.performance_tier,
        ap.gap_analysis_category,
        ap.avg_roas,
        ap.avg_ctr,
        ap.total_spend,
        ap.composite_performance_score,
        coalesce(ap.account_mapping_status, 'unmapped_account') as account_mapping_status,
        
        -- Traditional gap metrics
        case 
            when ap.persona_attribute is null then 'completely_unaddressed'
            when ap.gap_analysis_category = 'underperforming_persona' then 'underperforming'
            when ap.gap_analysis_category = 'opportunity_persona' then 'moderate_opportunity'
            when ap.gap_analysis_category = 'well_served_persona' then 'well_served'
            else 'unknown'
        end as gap_type,
        
        -- Traditional priority scoring  
        case 
            when ap.persona_attribute is null then rp.review_count * 20  -- Highest priority
            when ap.gap_analysis_category = 'underperforming_persona' then rp.review_count * 15
            when ap.gap_analysis_category = 'opportunity_persona' then rp.review_count * 10
            when ap.gap_analysis_category = 'well_served_persona' then rp.review_count * 2
            else rp.review_count * 1
        end as priority_score,
        
        -- Traditional opportunity assessment
        case 
            when ap.persona_attribute is null and rp.review_count >= 5 then 'high_opportunity'
            when ap.gap_analysis_category = 'underperforming_persona' and rp.review_count >= 3 then 'high_opportunity'
            when ap.persona_attribute is null and rp.review_count >= 2 then 'medium_opportunity'
            when ap.gap_analysis_category in ('underperforming_persona', 'opportunity_persona') then 'medium_opportunity'
            when ap.gap_analysis_category = 'well_served_persona' then 'low_opportunity'
            else 'unknown_opportunity'
        end as opportunity_level,
        
        -- Days since last review mention
        current_date - rp.latest_review_date as days_since_last_review,
        
        -- Review frequency (mentions per day)
        case 
            when rp.latest_review_date != rp.earliest_review_date 
            then rp.total_mentions::float / (rp.latest_review_date - rp.earliest_review_date + 1)
            else rp.total_mentions::float
        end as review_frequency
        
    from review_personas rp
    left join ad_performance_by_persona ap on rp.ad_account_id = ap.ad_account_id 
                                            and rp.persona_attribute = ap.persona_attribute
),

-- Create priority tiers
priority_tiers as (
    select
        *,
        case 
            when gap_type = 'completely_unaddressed' and opportunity_level = 'high_opportunity' then 'critical'
            when gap_type = 'underperforming' and opportunity_level = 'high_opportunity' then 'high'
            when gap_type = 'completely_unaddressed' and opportunity_level = 'medium_opportunity' then 'high'
            when gap_type = 'underperforming' and opportunity_level = 'medium_opportunity' then 'medium'
            when gap_type = 'moderate_opportunity' and opportunity_level in ('high_opportunity', 'medium_opportunity') then 'medium'
            when gap_type = 'well_served' then 'low'
            else 'unknown'
        end as gap_priority,
        
        -- Create actionable recommendations
        case 
            when gap_type = 'completely_unaddressed' then 'Create new ad campaigns targeting this persona'
            when gap_type = 'underperforming' then 'Optimize existing ads or create new creative approaches'
            when gap_type = 'moderate_opportunity' then 'Consider increasing budget or improving creative'
            when gap_type = 'well_served' then 'Maintain current approach'
            else 'Further analysis needed'
        end as recommendation,
        
        current_timestamp as loaded_at
    from gap_analysis
),

-- Add account context
final as (
    select
        pt.*,
        aa.ad_account_name,
        aa.account_industry,
        aa.account_description,
        
        -- Priority ranking for sorting
        case 
            when pt.gap_priority = 'critical' then 1
            when pt.gap_priority = 'high' then 2  
            when pt.gap_priority = 'medium' then 3
            else 5
        end as priority_rank
        
    from priority_tiers pt
    join {{ ref('stg_cad__ad_accounts') }} aa on pt.ad_account_id = aa.ad_account_id
    where pt.gap_priority in ('critical', 'high', 'medium')  -- Focus on actionable gaps
      and pt.review_count >= 1  -- At least one review mention
    order by priority_rank asc, pt.priority_score desc, pt.review_count desc
)

select * from final 