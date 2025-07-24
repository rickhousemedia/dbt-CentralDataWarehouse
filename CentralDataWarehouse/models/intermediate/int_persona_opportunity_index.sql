-- models/intermediate/int_persona_opportunity_index.sql
-- Calculate opportunity index for persona attributes: review_mentions / ad_spend
-- This replicates "Report 3: Opportunity Index Analysis" from the source app

{{ config(materialized='table') }}

with review_personas as (
    select
        persona_attribute,
        count(*) as review_count,
        count(*) * 100.0 / sum(count(*)) over() as review_percentage
    from {{ ref('int_review_persona_attributes') }}
    group by persona_attribute
),

ad_personas as (
    select
        persona_attribute,
        ad_count,
        total_ad_spend
    from {{ ref('int_persona_ad_performance') }}
),

-- Combine review and ad data
persona_combined as (
    select
        coalesce(rp.persona_attribute, ap.persona_attribute) as persona_attribute,
        coalesce(rp.review_count, 0) as review_count,
        coalesce(rp.review_percentage, 0) as review_percentage,
        coalesce(ap.ad_count, 0) as ad_count,
        coalesce(ap.total_ad_spend, 0) as total_ad_spend
    from review_personas rp
    full outer join ad_personas ap on lower(trim(rp.persona_attribute)) = lower(trim(ap.persona_attribute))
),

-- Calculate raw opportunity ratio
opportunity_ratios as (
    select
        persona_attribute,
        review_count,
        review_percentage,
        ad_count,
        total_ad_spend,
        case
            when total_ad_spend > 0 then review_count / total_ad_spend
            when review_count > 0 then 999999  -- High value for personas with reviews but no ad spend
            else 0
        end as raw_ratio
    from persona_combined
),

-- Get ratio statistics for normalization
ratio_stats as (
    select
        min(case when raw_ratio < 999999 then raw_ratio end) as min_finite_ratio,
        max(case when raw_ratio < 999999 then raw_ratio end) as max_finite_ratio
    from opportunity_ratios
    where raw_ratio > 0
)

-- Normalize ratios to 0-100 scale (same logic as source app)
select
    or_data.persona_attribute,
    or_data.review_count,
    or_data.review_percentage,
    or_data.ad_count,
    or_data.total_ad_spend,
    or_data.raw_ratio,
    case
        when or_data.raw_ratio >= 999999 then 100  -- Infinite ratio (reviews but no ad spend)
        when rs.max_finite_ratio > rs.min_finite_ratio then
            ((or_data.raw_ratio - rs.min_finite_ratio) / (rs.max_finite_ratio - rs.min_finite_ratio)) * 99
        else 50  -- All finite ratios are the same
    end as opportunity_index,
    
    -- Color coding (same logic as source app)
    case
        when (case
            when or_data.raw_ratio >= 999999 then 100
            when rs.max_finite_ratio > rs.min_finite_ratio then
                ((or_data.raw_ratio - rs.min_finite_ratio) / (rs.max_finite_ratio - rs.min_finite_ratio)) * 99
            else 50
        end) >= 90 then '#059669'  -- Dark green
        when (case
            when or_data.raw_ratio >= 999999 then 100
            when rs.max_finite_ratio > rs.min_finite_ratio then
                ((or_data.raw_ratio - rs.min_finite_ratio) / (rs.max_finite_ratio - rs.min_finite_ratio)) * 99
            else 50
        end) >= 80 then '#10b981'  -- Green
        when (case
            when or_data.raw_ratio >= 999999 then 100
            when rs.max_finite_ratio > rs.min_finite_ratio then
                ((or_data.raw_ratio - rs.min_finite_ratio) / (rs.max_finite_ratio - rs.min_finite_ratio)) * 99
            else 50
        end) >= 70 then '#34d399'  -- Medium green
        when (case
            when or_data.raw_ratio >= 999999 then 100
            when rs.max_finite_ratio > rs.min_finite_ratio then
                ((or_data.raw_ratio - rs.min_finite_ratio) / (rs.max_finite_ratio - rs.min_finite_ratio)) * 99
            else 50
        end) >= 60 then '#86efac'  -- Light green
        when (case
            when or_data.raw_ratio >= 999999 then 100
            when rs.max_finite_ratio > rs.min_finite_ratio then
                ((or_data.raw_ratio - rs.min_finite_ratio) / (rs.max_finite_ratio - rs.min_finite_ratio)) * 99
            else 50
        end) >= 50 then '#bef264'  -- Yellow-green
        when (case
            when or_data.raw_ratio >= 999999 then 100
            when rs.max_finite_ratio > rs.min_finite_ratio then
                ((or_data.raw_ratio - rs.min_finite_ratio) / (rs.max_finite_ratio - rs.min_finite_ratio)) * 99
            else 50
        end) >= 40 then '#facc15'  -- Yellow
        when (case
            when or_data.raw_ratio >= 999999 then 100
            when rs.max_finite_ratio > rs.min_finite_ratio then
                ((or_data.raw_ratio - rs.min_finite_ratio) / (rs.max_finite_ratio - rs.min_finite_ratio)) * 99
            else 50
        end) >= 30 then '#fb923c'  -- Light orange
        when (case
            when or_data.raw_ratio >= 999999 then 100
            when rs.max_finite_ratio > rs.min_finite_ratio then
                ((or_data.raw_ratio - rs.min_finite_ratio) / (rs.max_finite_ratio - rs.min_finite_ratio)) * 99
            else 50
        end) >= 20 then '#f97316'  -- Orange
        when (case
            when or_data.raw_ratio >= 999999 then 100
            when rs.max_finite_ratio > rs.min_finite_ratio then
                ((or_data.raw_ratio - rs.min_finite_ratio) / (rs.max_finite_ratio - rs.min_finite_ratio)) * 99
            else 50
        end) >= 10 then '#ef4444'  -- Light red
        when (case
            when or_data.raw_ratio >= 999999 then 100
            when rs.max_finite_ratio > rs.min_finite_ratio then
                ((or_data.raw_ratio - rs.min_finite_ratio) / (rs.max_finite_ratio - rs.min_finite_ratio)) * 99
            else 50
        end) >= 5 then '#dc2626'  -- Red
        else '#991b1b'  -- Dark red
    end as opportunity_color

from opportunity_ratios or_data
cross join ratio_stats rs
order by 
    case
        when or_data.raw_ratio >= 999999 then 100
        when rs.max_finite_ratio > rs.min_finite_ratio then
            ((or_data.raw_ratio - rs.min_finite_ratio) / (rs.max_finite_ratio - rs.min_finite_ratio)) * 99
        else 50
    end desc 