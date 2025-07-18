-- models/staging/stg_cad__ad_accounts.sql
-- This model cleans and standardizes the ad_accounts table from the CAD tool source.

with source as (

    select * from {{ source('raw_cad_public', 'ad_accounts') }}

),

renamed as (

    select
        -- IDs
        id as ad_account_id,
        company_id,
        ad_account_id as meta_ad_account_id, -- The platform-specific ID

        -- Account Details
        ad_account_name,
        account_name,
        account_email,
        account_industry,
        account_description

    from source

)

select * from renamed
