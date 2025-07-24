-- models/intermediate/int_persona_review_ranking.sql
-- Persona attributes ranked by review count
-- This replicates "Report 1: Clusters Ranked by Review Count" from the source app

{{ config(materialized='table') }}

with persona_counts as (
    select
        persona_attribute,
        ad_account_id,
        count(*) as review_count
    from {{ ref('int_review_persona_attributes') }}
    group by persona_attribute, ad_account_id
),

total_reviews as (
    select count(*) as total_unique_reviews
    from {{ ref('int_review_persona_attributes') }}
),

-- Add account information
persona_with_accounts as (
    select
        pc.persona_attribute,
        pc.ad_account_id,
        aa.ad_account_name,
        pc.review_count,
        tr.total_unique_reviews,
        (pc.review_count * 100.0 / tr.total_unique_reviews) as percentage
    from persona_counts pc
    cross join total_reviews tr
    left join {{ ref('stg_cad__ad_accounts') }} aa on pc.ad_account_id = aa.ad_account_id
)

select
    persona_attribute as persona_name,
    coalesce(ad_account_name, 'Account ' || ad_account_id) as account_name,
    ad_account_id,
    review_count,
    percentage,
    total_unique_reviews,
    
    -- Add some basic analysis
    case
        when percentage >= 5.0 then 'High Volume'
        when percentage >= 2.0 then 'Medium Volume'
        when percentage >= 1.0 then 'Low Volume'
        else 'Minimal Volume'
    end as volume_category,
    
    rank() over (order by review_count desc) as review_rank

from persona_with_accounts
order by review_count desc, persona_attribute 