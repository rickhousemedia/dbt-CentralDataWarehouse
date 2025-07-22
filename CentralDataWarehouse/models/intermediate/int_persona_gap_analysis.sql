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

-- Use cross-account persona mapping for enhanced gap analysis
cross_account_personas as (
    select
        cad_account_id,
        persona_attribute,
        persona_coverage,
        cad_persona_mentions,
        cad_ads_with_persona,
        review_persona_mentions,
        review_count_with_persona,
        avg_review_confidence,
        latest_review_mention as latest_review_date,
        cross_system_alignment_score,
        review_account_name,
        review_account_id,
        cad_account_name,
        case 
            when latest_review_mention is not null 
            then latest_review_mention - interval '30 days'  -- Estimate earliest as 30 days before latest
            else null 
        end as earliest_review_date
    from {{ ref('int_cross_account_persona_mapping') }}
    union all
    -- Add unmapped review personas
    select
        rpa.ad_account_id as cad_account_id,
        rpa.persona_attribute,
        'review_only_unmapped' as persona_coverage,
        0 as cad_persona_mentions,
        0 as cad_ads_with_persona,
        count(*) as review_persona_mentions,
        count(distinct rpa.review_id) as review_count_with_persona,
        avg(rpa.confidence_score) as avg_review_confidence,
        max(rpa.review_date) as latest_review_date,
        0 as cross_system_alignment_score,
        null as review_account_name,
        null as review_account_id,
        null as cad_account_name,
        min(rpa.review_date) as earliest_review_date
    from {{ ref('int_review_persona_attributes') }} rpa
    left join {{ ref('CAD_Reviews_DWH_mapping_table') }} map 
        on rpa.ad_account_id = map.review_account_id
    where map.review_account_id is null  -- Only unmapped accounts
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
        cp.cad_account_id,
        cp.persona_attribute,
        cp.review_count_with_persona as review_count,
        cp.review_persona_mentions as total_mentions,
        cp.avg_review_confidence as avg_confidence,
        cp.latest_review_date,
        cp.earliest_review_date,
        cp.persona_coverage,
        cp.cross_system_alignment_score,
        cp.review_account_name,
        cp.review_account_id,
        coalesce(cp.cad_account_name, 'Unknown') as cad_account_name,
        
        -- Ad performance data (null if persona not in ads)
        coalesce(ap.ad_count, 0) as ad_count,
        ap.performance_tier,
        ap.gap_analysis_category,
        ap.avg_roas,
        ap.avg_ctr,
        ap.total_spend,
        ap.composite_performance_score,
        coalesce(ap.account_mapping_status, 'unmapped_account') as account_mapping_status,
        
        -- Enhanced gap metrics with cross-account awareness
        case 
            when cp.persona_coverage = 'review_only' or cp.persona_coverage = 'review_only_unmapped' then 'completely_unaddressed'
            when ap.gap_analysis_category = 'underperforming_persona' then 'underperforming'
            when ap.gap_analysis_category = 'opportunity_persona' then 'moderate_opportunity'
            when ap.gap_analysis_category = 'well_served_persona' then 'well_served'
            when cp.persona_coverage = 'cad_only' then 'ads_without_reviews'
            else 'unknown'
        end as gap_type,
        
        -- Enhanced priority scoring with cross-account boost
        case 
            when cp.persona_coverage in ('review_only', 'review_only_unmapped') 
            then cp.review_count_with_persona * 25 * case when cp.cross_system_alignment_score > 0 then 1.5 else 1.0 end
            when ap.gap_analysis_category = 'underperforming_persona' 
            then cp.review_count_with_persona * 20 * case when cp.cross_system_alignment_score > 0 then 1.3 else 1.0 end
            when ap.gap_analysis_category = 'opportunity_persona' 
            then cp.review_count_with_persona * 15 * case when cp.cross_system_alignment_score > 0 then 1.2 else 1.0 end
            when ap.gap_analysis_category = 'well_served_persona' 
            then cp.review_count_with_persona * 5
            when cp.persona_coverage = 'cad_only'
            then cp.cad_persona_mentions * 3  -- Lower priority for ads without review validation
            else cp.review_count_with_persona * 1
        end as priority_score,
        
        -- Enhanced opportunity assessment
        case 
            when cp.persona_coverage in ('review_only', 'review_only_unmapped') and cp.review_count_with_persona >= 5 then 'critical_opportunity'
            when cp.persona_coverage in ('review_only', 'review_only_unmapped') and cp.review_count_with_persona >= 2 then 'high_opportunity'
            when ap.gap_analysis_category = 'underperforming_persona' and cp.review_count_with_persona >= 3 then 'high_opportunity'
            when ap.gap_analysis_category in ('underperforming_persona', 'opportunity_persona') and cp.cross_system_alignment_score > 0 then 'medium_opportunity_mapped'
            when ap.gap_analysis_category in ('underperforming_persona', 'opportunity_persona') then 'medium_opportunity'
            when ap.gap_analysis_category = 'well_served_persona' then 'low_opportunity'
            when cp.persona_coverage = 'cad_only' then 'validation_needed'
            else 'unknown_opportunity'
        end as opportunity_level,
        
        -- Days since last review mention
        current_date - cp.latest_review_date as days_since_last_review,
        
        -- Review frequency (mentions per day)
        case 
            when cp.latest_review_date != cp.earliest_review_date and cp.earliest_review_date is not null
            then cp.review_persona_mentions::float / (cp.latest_review_date - cp.earliest_review_date + 1)
            else cp.review_persona_mentions::float
        end as review_frequency
        
    from cross_account_personas cp
    left join ad_performance_by_persona ap on cp.cad_account_id = ap.ad_account_id 
                                            and cp.persona_attribute = ap.persona_attribute
),

-- Create enhanced priority tiers with cross-account awareness
priority_tiers as (
    select
        *,
        case 
            when gap_type = 'completely_unaddressed' and opportunity_level = 'critical_opportunity' then 'critical'
            when gap_type = 'completely_unaddressed' and opportunity_level = 'high_opportunity' then 'critical'
            when gap_type = 'underperforming' and opportunity_level = 'high_opportunity' then 'high'
            when gap_type = 'completely_unaddressed' and opportunity_level = 'medium_opportunity_mapped' then 'high'
            when gap_type = 'underperforming' and opportunity_level in ('medium_opportunity_mapped', 'medium_opportunity') then 'medium'
            when gap_type = 'moderate_opportunity' and opportunity_level in ('high_opportunity', 'medium_opportunity_mapped') then 'medium'
            when gap_type = 'moderate_opportunity' and opportunity_level = 'medium_opportunity' then 'low'
            when gap_type = 'well_served' then 'low'
            when gap_type = 'ads_without_reviews' then 'validation_needed'
            else 'unknown'
        end as gap_priority,
        
        -- Create enhanced actionable recommendations
        case 
            when gap_type = 'completely_unaddressed' and account_mapping_status = 'mapped_account'
            then 'CRITICAL: Create new ad campaigns targeting this persona (validated across systems)'
            when gap_type = 'completely_unaddressed' 
            then 'HIGH: Create new ad campaigns targeting this persona'
            when gap_type = 'underperforming' and account_mapping_status = 'mapped_account'
            then 'OPTIMIZE: Improve existing ads with cross-account insights'
            when gap_type = 'underperforming' 
            then 'OPTIMIZE: Improve existing ads or create new creative approaches'
            when gap_type = 'moderate_opportunity' and account_mapping_status = 'mapped_account'
            then 'ENHANCE: Scale successful approach with cross-system validation'
            when gap_type = 'moderate_opportunity' 
            then 'ENHANCE: Consider increasing budget or improving creative'
            when gap_type = 'well_served' 
            then 'MAINTAIN: Continue current successful approach'
            when gap_type = 'ads_without_reviews'
            then 'VALIDATE: Check if ad personas resonate with actual customers'
            else 'INVESTIGATE: Further analysis needed'
        end as recommendation,
        
        current_timestamp as loaded_at
    from gap_analysis
),

-- Add account context with cross-system information
final as (
    select
        pt.*,
        aa.ad_account_name,
        aa.account_industry,
        aa.account_description,
        
        -- Cross-system context
        case 
            when pt.review_account_name is not null 
            then concat(aa.ad_account_name, ' ↔ ', pt.review_account_name)
            else aa.ad_account_name
        end as cross_system_account_summary,
        
        -- Enhanced filtering for actionable insights
        case 
            when pt.gap_priority = 'critical' then 1
            when pt.gap_priority = 'high' then 2  
            when pt.gap_priority = 'medium' then 3
            when pt.gap_priority = 'validation_needed' then 4
            else 5
        end as priority_rank
        
    from priority_tiers pt
    join {{ ref('stg_cad__ad_accounts') }} aa on pt.cad_account_id = aa.ad_account_id
    where pt.gap_priority in ('critical', 'high', 'medium', 'validation_needed')  -- Focus on actionable gaps
      and (pt.review_count >= 1 or pt.gap_type = 'ads_without_reviews')  -- Include validation-needed items
    order by priority_rank asc, pt.priority_score desc, pt.review_count desc
)

select * from final 