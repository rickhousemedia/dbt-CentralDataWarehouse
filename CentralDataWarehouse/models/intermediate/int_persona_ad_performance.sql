-- models/intermediate/int_persona_ad_performance.sql
-- Join persona attributes from ads with performance metrics
-- This enables "Report 2: Persona attributes ranked by ad spend with performance metrics"

{{ config(materialized='table') }}

with ad_personas as (
    select * from {{ ref('int_ad_persona_attributes') }}
),

ad_performance as (
    select
        ai.ads_meta_id,
        cast(ai.amount_spent as numeric) as amount_spent,
        cast(ai.omni_purchase_value as numeric) as omni_purchase_value,
        cast(ai.omni_purchase_purchases as numeric) as omni_purchase_purchases,
        cast(ai.video_3_sec_watched_actions as numeric) as video_3_sec_watched_actions,
        cast(ai.impressions as numeric) as impressions,
        cast(ai.roas as numeric) as ad_roas,
        cast(ai.ctr as numeric) as ctr,
        ai.loaded_at
    from {{ ref('stg_cad__ads_insights_meta') }} ai
),

-- Join personas with performance data (ad personas already have ads_meta_id)
persona_performance as (
    select
        ap.persona_attribute,
        ap.persona_value,
        ap.confidence_score,
        ap.ad_account_id,
        ap.ads_meta_id,
        perf.amount_spent,
        perf.omni_purchase_value,
        perf.omni_purchase_purchases,
        perf.video_3_sec_watched_actions,
        perf.impressions,
        perf.ad_roas,
        perf.ctr
    from ad_personas ap
    inner join ad_performance perf on ap.ads_meta_id = perf.ads_meta_id
),

-- Aggregate by persona attribute
persona_aggregated as (
    select
        persona_attribute,
        count(distinct ads_meta_id) as ad_count,
        sum(amount_spent) as total_ad_spend,
        sum(omni_purchase_value) as total_purchase_value,
        sum(omni_purchase_purchases) as total_purchases,
        sum(video_3_sec_watched_actions) as total_video_watches,
        sum(impressions) as total_impressions,
        avg(confidence_score) as avg_confidence,
        count(*) as persona_mentions
    from persona_performance
    group by persona_attribute
)

select
    persona_attribute,
    ad_count,
    total_ad_spend,
    persona_mentions,
    avg_confidence,
    
    -- Calculate performance metrics (same logic as source app)
    case 
        when total_purchases > 0 then total_purchase_value / total_purchases 
        else 0 
    end as aov,
    
    case 
        when total_ad_spend > 0 then total_purchase_value / total_ad_spend 
        else 0 
    end as roas,
    
    case 
        when total_impressions > 0 then total_video_watches / total_impressions 
        else 0 
    end as thumbstop_rate,
    
    total_purchase_value,
    total_purchases,
    total_video_watches,
    total_impressions
    
from persona_aggregated
order by total_ad_spend desc 