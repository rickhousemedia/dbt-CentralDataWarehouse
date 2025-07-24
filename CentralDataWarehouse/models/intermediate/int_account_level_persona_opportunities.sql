-- models/intermediate/int_account_level_persona_opportunities.sql
-- Account-level persona opportunity analysis using product mapping
-- Since account IDs don't match directly, we'll analyze at account level using the mapping

{{ config(materialized='table') }}

with mapped_cad_accounts as (
    -- CAD accounts that have review mappings
    select distinct
        ad_account_id as cad_account_id,
        ad_account_name as cad_account_name,
        count(*) as mapped_products
    from {{ ref('CAD_Reviews_DWH_mapping_table') }}
    where matched_review_product is not null 
      and review_account_id is not null
    group by ad_account_id, ad_account_name
),

mapped_review_accounts as (
    -- Review accounts that have CAD mappings
    select distinct
        review_account_id,
        review_account_name,
        count(*) as mapped_products
    from {{ ref('CAD_Reviews_DWH_mapping_table') }}
    where matched_review_product is not null 
      and review_account_id is not null
    group by review_account_id, review_account_name
),

-- All review personas (regardless of mapping)
all_review_personas as (
    select 
        rpa.ad_account_id as review_account_id,
        count(distinct rpa.persona_attribute) as unique_personas,
        count(*) as total_persona_mentions,
        avg(rpa.confidence_score) as avg_confidence,
        max(rpa.review_date) as latest_review_date,
        -- Sample personas for this account
        string_agg(distinct rpa.persona_attribute, ', ') as sample_personas
    from {{ ref('int_review_persona_attributes') }} rpa
    group by rpa.ad_account_id
),

-- CAD account performance
cad_account_performance as (
    select 
        a.ad_account_id,
        aa.ad_account_name,
        sum(ai.amount_spent::numeric) as total_ad_spend,
        sum(ai.omni_purchase_value::numeric) as total_purchase_value,
        sum(ai.omni_purchase_purchases::numeric) as total_purchases,
        count(distinct am.ad_id) as total_ads,
        -- Performance metrics
        case when sum(ai.omni_purchase_purchases::numeric) > 0 
             then sum(ai.omni_purchase_value::numeric) / sum(ai.omni_purchase_purchases::numeric) 
             else 0 end as account_aov,
        case when sum(ai.amount_spent::numeric) > 0 
             then sum(ai.omni_purchase_value::numeric) / sum(ai.amount_spent::numeric) 
             else 0 end as account_roas,
        case when sum(ai.impressions::numeric) > 0 
             then sum(ai.video_3_sec_watched_actions::numeric) / sum(ai.impressions::numeric) 
             else 0 end as account_thumbstop_rate
    from {{ ref('stg_cad__ads_insights_meta') }} ai
    inner join {{ ref('stg_cad__ads_meta') }} am on ai.ads_meta_id = am.ads_meta_id
    inner join {{ ref('stg_cad__ads') }} a on am.ad_id = a.ad_id
    inner join {{ ref('stg_cad__ad_accounts') }} aa on a.ad_account_id = aa.ad_account_id
    group by a.ad_account_id, aa.ad_account_name
),

-- Ad personas from CAD accounts (via Review analysis)
cad_ad_personas as (
    select 
        apa.ad_account_id as cad_account_id,
        count(distinct apa.persona_attribute) as unique_ad_personas,
        count(*) as total_ad_persona_mentions,
        avg(apa.confidence_score) as avg_ad_confidence
    from {{ ref('int_ad_persona_attributes_from_review_analysis') }} apa
    group by apa.ad_account_id
)

-- Final analysis combining all data
select 
    -- Account identification
    coalesce(cap.ad_account_id, mca.cad_account_id) as cad_account_id,
    coalesce(cap.ad_account_name, mca.cad_account_name) as cad_account_name,
    
    -- Mapping status
    case when mca.cad_account_id is not null then 'Has Review Mapping' 
         else 'No Review Mapping' end as mapping_status,
    coalesce(mca.mapped_products, 0) as mapped_products,
    
    -- Ad performance
    coalesce(cap.total_ad_spend, 0) as total_ad_spend,
    coalesce(cap.account_aov, 0) as account_aov,
    coalesce(cap.account_roas, 0) as account_roas,
    coalesce(cap.account_thumbstop_rate, 0) as account_thumbstop_rate,
    coalesce(cap.total_ads, 0) as total_ads,
    
    -- Ad persona data
    coalesce(adp.unique_ad_personas, 0) as unique_ad_personas,
    coalesce(adp.total_ad_persona_mentions, 0) as total_ad_persona_mentions,
    
    -- Review persona insights (for mapped accounts, show aggregate insights)
    case when mca.cad_account_id is not null then
        (select count(distinct persona_attribute) 
         from {{ ref('int_review_persona_attributes') }} 
         where ad_account_id in (select review_account_id from mapped_review_accounts))
         else 0 end as review_personas_available,
         
    case when mca.cad_account_id is not null then
        (select count(*) 
         from {{ ref('int_review_persona_attributes') }} 
         where ad_account_id in (select review_account_id from mapped_review_accounts))
         else 0 end as total_review_mentions,
    
    -- Opportunity scoring
    case 
        when mca.cad_account_id is not null and cap.total_ad_spend > 0 then
            -- Has mapping and ad spend: calculate opportunity based on review vs ad personas
            case when coalesce(adp.unique_ad_personas, 0) = 0 then 100  -- No ad personas but has reviews
                 else greatest(0, 100 - (adp.unique_ad_personas * 10)) end  -- Reduce score based on ad persona coverage
        when mca.cad_account_id is not null then 90  -- Has mapping but no ad spend
        when cap.total_ad_spend > 0 then 20  -- Has ad spend but no review insights
        else 0  -- No mapping and no ad spend
    end as opportunity_score,
    
    -- Performance tier
    case 
        when cap.account_roas >= 3 then 'High Performer'
        when cap.account_roas >= 1.5 then 'Medium Performer'  
        when cap.account_roas > 0 then 'Low Performer'
        else 'No Performance Data'
    end as performance_tier,
    
    -- Priority ranking
    (coalesce(mca.mapped_products, 0) * 10 +  -- Boost mapped accounts
     case when cap.account_roas >= 2 then 50 else 0 end +  -- Boost high performers
     least(cap.total_ad_spend / 1000, 50) +  -- Boost based on spend (capped)
     case when coalesce(adp.unique_ad_personas, 0) = 0 then 30 else 0 end  -- Boost accounts with no persona targeting
    ) as priority_score

from cad_account_performance cap
full outer join mapped_cad_accounts mca on cap.ad_account_id = mca.cad_account_id
left join cad_ad_personas adp on coalesce(cap.ad_account_id, mca.cad_account_id) = adp.cad_account_id

order by priority_score desc, total_ad_spend desc 