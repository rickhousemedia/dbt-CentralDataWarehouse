-- models/intermediate/int_persona_cross_system_analysis.sql
-- Cross-system persona analysis using product mapping table
-- Links Review personas (customer demand) with CAD ad performance (supply) through product matching

{{ config(materialized='table') }}

with product_mapping as (
    select * from {{ ref('CAD_Reviews_DWH_mapping_table') }}
    where matched_review_product is not null 
      and review_account_id is not null
      and ad_account_id is not null
),

-- Review personas from customer reviews (demand side)
review_personas as (
    select 
        rpa.ad_account_id as review_account_id,
        rpa.persona_attribute,
        count(*) as review_mentions,
        avg(rpa.confidence_score) as avg_review_confidence,
        max(rpa.review_date) as latest_review_date,
        'customer_demand' as persona_source
    from {{ ref('int_review_persona_attributes') }} rpa
    group by rpa.ad_account_id, rpa.persona_attribute
),

-- Ad personas from Review tool's analysis of CAD ads (supply side) 
ad_personas as (
    select 
        apa.ad_account_id,
        apa.persona_attribute,
        count(*) as ad_mentions,
        avg(apa.confidence_score) as avg_ad_confidence,
        'ad_targeting' as persona_source
    from {{ ref('int_ad_persona_attributes_from_review_analysis') }} apa
    group by apa.ad_account_id, apa.persona_attribute
),

-- Get ad performance by account (since we can't link individual ads yet)
account_performance as (
    select 
        am.ad_id,
        a.ad_account_id,
        ai.amount_spent::numeric as amount_spent,
        ai.omni_purchase_value::numeric as omni_purchase_value,
        ai.omni_purchase_purchases::numeric as omni_purchase_purchases,
        ai.video_3_sec_watched_actions::numeric as video_3_sec_watched_actions,
        ai.impressions::numeric as impressions,
        ai.roas::numeric as roas
    from {{ ref('stg_cad__ads_insights_meta') }} ai
    inner join {{ ref('stg_cad__ads_meta') }} am on ai.ads_meta_id = am.ads_meta_id
    inner join {{ ref('stg_cad__ads') }} a on am.ad_id = a.ad_id
),

account_performance_summary as (
    select 
        ad_account_id,
        sum(amount_spent) as total_ad_spend,
        sum(omni_purchase_value) as total_purchase_value,
        sum(omni_purchase_purchases) as total_purchases,
        sum(video_3_sec_watched_actions) as total_video_watches,
        sum(impressions) as total_impressions,
        count(distinct ad_id) as total_ads,
        -- Calculate performance metrics
        case when sum(omni_purchase_purchases) > 0 
             then sum(omni_purchase_value) / sum(omni_purchase_purchases) 
             else 0 end as account_aov,
        case when sum(amount_spent) > 0 
             then sum(omni_purchase_value) / sum(amount_spent) 
             else 0 end as account_roas,
        case when sum(impressions) > 0 
             then sum(video_3_sec_watched_actions) / sum(impressions) 
             else 0 end as account_thumbstop_rate
    from account_performance
    group by ad_account_id
),

-- Link review personas to CAD performance through product mapping
mapped_review_personas as (
    select 
        pm.ad_account_id as cad_account_id,
        pm.ad_account_name as cad_account_name,
        pm.product as cad_product,
        pm.review_account_id,
        pm.review_account_name,
        pm.matched_review_product,
        rp.persona_attribute,
        rp.review_mentions,
        rp.avg_review_confidence,
        rp.latest_review_date,
        aps.total_ad_spend,
        aps.account_aov,
        aps.account_roas,
        aps.account_thumbstop_rate,
        aps.total_ads,
        -- Calculate opportunity metrics
        case when aps.total_ad_spend > 0 
             then rp.review_mentions / aps.total_ad_spend 
             else 999999 end as raw_opportunity_ratio
    from product_mapping pm
    inner join review_personas rp on pm.review_account_id = rp.review_account_id
    left join account_performance_summary aps on pm.ad_account_id = aps.ad_account_id
),

-- Also get ad personas for the same mapped accounts
mapped_ad_personas as (
    select 
        pm.ad_account_id as cad_account_id,
        pm.ad_account_name as cad_account_name,
        pm.product as cad_product,
        ap.persona_attribute,
        ap.ad_mentions,
        ap.avg_ad_confidence,
        'currently_targeted' as persona_status
    from product_mapping pm
    inner join ad_personas ap on pm.ad_account_id = ap.ad_account_id
),

-- Combine to create comprehensive persona analysis
persona_analysis as (
    select 
        mrp.cad_account_id,
        mrp.cad_account_name,
        mrp.cad_product,
        mrp.review_account_name,
        mrp.persona_attribute,
        mrp.review_mentions,
        mrp.avg_review_confidence,
        mrp.latest_review_date,
        coalesce(map.ad_mentions, 0) as ad_mentions,
        coalesce(map.avg_ad_confidence, 0) as avg_ad_confidence,
        mrp.total_ad_spend,
        mrp.account_aov,
        mrp.account_roas,
        mrp.account_thumbstop_rate,
        mrp.total_ads,
        mrp.raw_opportunity_ratio,
        
        -- Determine persona gap status
        case 
            when map.ad_mentions > 0 then 'addressed_in_ads'
            when mrp.review_mentions > 0 then 'unaddressed_opportunity'
            else 'unknown'
        end as gap_status,
        
        -- Enhanced priority scoring
        (mrp.review_mentions * mrp.avg_review_confidence * 
         case when map.ad_mentions > 0 then 0.5 else 2.0 end * -- Boost unaddressed personas
         case when mrp.account_roas > 2 then 1.5 else 1.0 end   -- Boost high-performing accounts
        ) as priority_score
        
    from mapped_review_personas mrp
    left join mapped_ad_personas map on mrp.cad_account_id = map.cad_account_id 
                                      and lower(trim(mrp.persona_attribute)) = lower(trim(map.persona_attribute))
)

select 
    cad_account_id,
    cad_account_name,
    cad_product,
    review_account_name,
    persona_attribute,
    review_mentions,
    avg_review_confidence,
    latest_review_date,
    ad_mentions,
    avg_ad_confidence,
    total_ad_spend,
    account_aov,
    account_roas,
    account_thumbstop_rate,
    total_ads,
    raw_opportunity_ratio,
    gap_status,
    priority_score,
    
    -- Add ranking for easy sorting
    row_number() over (order by priority_score desc) as opportunity_rank
    
from persona_analysis
order by priority_score desc, review_mentions desc 