-- models/staging/stg_cad__ads_insights_meta.sql
-- This model cleans and standardizes the ads_insights_meta table from the CAD tool source.
-- This table contains key performance metrics for ads that will be used for gap analysis.

with source as (

    select * from {{ source('raw_cad_public', 'ads_insights_meta') }}

),

renamed as (

    select
        -- IDs
        id as ad_insights_id,
        ads_meta_id,

        -- Core Metrics
        reach,
        impressions,
        click,
        amount_spent,
        results,
        
        -- Cost Metrics
        cpc,
        cpm,
        cpp,
        cost_per_result,
        
        -- Rate Metrics
        ctr,
        frequency,
        thumbstop,
        retention,
        
        -- ROAS & Purchase Metrics
        roas,
        omni_purchase_roas_1d_click,
        omni_purchase_roas_7d_click,
        omni_purchase_roas_1d_view,
        omni_purchase_value,
        omni_purchase_value_1d_click,
        omni_purchase_value_7d_click,
        omni_purchase_value_1d_view,
        omni_purchase_purchases,
        omni_purchase_purchases_1d_click,
        omni_purchase_purchases_7d_click,
        omni_purchase_purchases_1d_view,
        
        -- Video Metrics
        video_30_sec_watched_actions,
        video_3_sec_watched_actions,
        
        -- Hit Rate Metrics
        hit_rate_x,
        hit_rate_y,
        hit_rate_z,
        hit_rate,
        
        -- Campaign Info
        objective,
        
        -- Metadata
        current_timestamp as loaded_at

    from source

)

select * from renamed 