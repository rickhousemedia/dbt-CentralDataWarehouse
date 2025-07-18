-- models/intermediate/int_persona_performance_scorecard.sql
-- This model creates scorecards linking persona attributes to ad performance metrics

{{ config(
    materialized='table',
    indexes=[
        {'columns': ['ad_account_id'], 'type': 'btree'},
        {'columns': ['persona_attribute'], 'type': 'btree'},
        {'columns': ['performance_tier'], 'type': 'btree'}
    ]
) }}

with ad_personas_with_performance as (
    select
        apa.ad_account_id,
        apa.persona_attribute,
        apa.persona_value,
        apa.confidence_score,
        apa.ads_meta_id,
        apa.ad_analysis_id,
        apa.core_persona,
        -- Performance metrics
        ai.roas,
        ai.ctr,
        ai.cpc,
        ai.amount_spent,
        ai.impressions,
        ai.reach,
        ai.click,
        ai.thumbstop,
        ai.retention,
        ai.frequency,
        ai.objective,
        apa.loaded_at
    from {{ ref('int_ad_persona_attributes') }} apa
    join {{ ref('stg_cad__ads_insights_meta') }} ai on apa.ads_meta_id = ai.ads_meta_id
),

-- Calculate performance metrics per persona attribute
persona_performance_metrics as (
    select
        ad_account_id,
        persona_attribute,
        persona_value,
        count(distinct ads_meta_id) as ad_count,
        count(distinct core_persona) as core_persona_count,
        
        -- ROAS metrics
        avg(roas) as avg_roas,
        percentile_cont(0.5) within group (order by roas) as median_roas,
        max(roas) as max_roas,
        min(roas) as min_roas,
        
        -- CTR metrics  
        avg(ctr) as avg_ctr,
        percentile_cont(0.5) within group (order by ctr) as median_ctr,
        max(ctr) as max_ctr,
        
        -- Cost metrics
        avg(cpc) as avg_cpc,
        percentile_cont(0.5) within group (order by cpc) as median_cpc,
        
        -- Volume metrics
        sum(amount_spent) as total_spend,
        sum(impressions) as total_impressions,
        sum(reach) as total_reach,
        sum(click) as total_clicks,
        
        -- Engagement metrics
        avg(thumbstop) as avg_thumbstop,
        avg(retention) as avg_retention,
        avg(frequency) as avg_frequency,
        
        -- Confidence metrics
        avg(confidence_score) as avg_confidence_score,
        
        -- Time metrics
        max(loaded_at) as latest_data_date
    from ad_personas_with_performance
    where roas is not null
    group by 1, 2, 3
),

-- Create performance tiers
performance_tiers as (
    select
        *,
        case 
            when avg_roas >= 3.0 then 'high_performer'
            when avg_roas >= 1.5 then 'medium_performer'
            when avg_roas >= 0.5 then 'low_performer'
            else 'poor_performer'
        end as roas_tier,
        case 
            when avg_ctr >= 2.0 then 'high_engagement'
            when avg_ctr >= 1.0 then 'medium_engagement'
            when avg_ctr >= 0.5 then 'low_engagement'
            else 'poor_engagement'
        end as ctr_tier,
        case 
            when total_spend >= 10000 then 'high_spend'
            when total_spend >= 5000 then 'medium_spend'
            when total_spend >= 1000 then 'low_spend'
            else 'minimal_spend'
        end as spend_tier
    from persona_performance_metrics
),

-- Create overall performance tier
performance_scoring as (
    select
        *,
        case 
            when roas_tier = 'high_performer' and ctr_tier in ('high_engagement', 'medium_engagement') then 'top_performer'
            when roas_tier = 'high_performer' or (roas_tier = 'medium_performer' and ctr_tier = 'high_engagement') then 'strong_performer'
            when roas_tier = 'medium_performer' then 'moderate_performer'
            when roas_tier = 'low_performer' then 'weak_performer'
            else 'poor_performer'
        end as performance_tier,
        
        -- Calculate composite score (weighted average)
        (
            (avg_roas * 0.4) +  -- ROAS weight 40%
            (avg_ctr * 10 * 0.3) +  -- CTR weight 30% (scaled up)
            (avg_thumbstop * 0.2) +  -- Thumbstop weight 20%
            (avg_retention * 0.1)  -- Retention weight 10%
        ) as composite_performance_score
    from performance_tiers
),

-- Get review context for gap analysis
review_context as (
    select
        ad_account_id,
        persona_attribute,
        count(distinct review_id) as review_mention_count,
        avg(confidence_score) as avg_review_confidence,
        max(review_date) as latest_review_date
    from {{ ref('int_review_persona_attributes') }}
    group by 1, 2
),

-- Final scorecard
final as (
    select
        ps.*,
        coalesce(rc.review_mention_count, 0) as review_mention_count,
        rc.avg_review_confidence,
        rc.latest_review_date,
        
        -- Gap analysis flags
        case 
            when rc.review_mention_count > 0 and ps.performance_tier in ('poor_performer', 'weak_performer') 
            then 'underperforming_persona'
            when rc.review_mention_count > 0 and ps.performance_tier in ('top_performer', 'strong_performer') 
            then 'well_served_persona'
            when rc.review_mention_count > 0 and ps.performance_tier = 'moderate_performer' 
            then 'opportunity_persona'
            when rc.review_mention_count > 0 and ps.ad_count = 0 
            then 'unaddressed_persona'
            else 'unknown_persona'
        end as gap_analysis_category,
        
        -- Opportunity score (higher = better opportunity)
        case 
            when rc.review_mention_count > 0 and ps.performance_tier in ('poor_performer', 'weak_performer') 
            then rc.review_mention_count * 10  -- High opportunity
            when rc.review_mention_count > 0 and ps.performance_tier = 'moderate_performer' 
            then rc.review_mention_count * 5   -- Medium opportunity
            when rc.review_mention_count > 0 and ps.ad_count = 0 
            then rc.review_mention_count * 15  -- Very high opportunity
            else 0
        end as opportunity_score,
        
        current_timestamp as loaded_at
    from performance_scoring ps
    left join review_context rc on ps.ad_account_id = rc.ad_account_id 
                                and ps.persona_attribute = rc.persona_attribute
)

select * from final 