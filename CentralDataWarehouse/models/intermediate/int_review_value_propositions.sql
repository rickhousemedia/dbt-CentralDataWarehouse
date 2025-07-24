-- models/intermediate/int_review_value_propositions.sql
-- This model normalizes JSON value propositions from customer reviews into structured rows

{{ config(
    materialized='table',
    indexes=[
        {'columns': ['ad_account_id'], 'type': 'btree'},
        {'columns': ['value_proposition'], 'type': 'btree'},
        {'columns': ['review_date'], 'type': 'btree'}
    ]
) }}

with review_value_props as (
    select
        review_id,
        ad_account_id,
        review_date,
        review_author,
        r_value_propositions,
        loaded_at
    from {{ ref('stg_review__reviews') }}
    where r_value_propositions is not null
),

-- Extract value propositions from JSON array
normalized_value_props as (
    select
        review_id,
        ad_account_id,
        review_date,
        review_author,
        value_prop_text as value_proposition,
        '3' as importance_score,  -- Default importance
        'positive' as sentiment,  -- Default sentiment
        loaded_at
    from review_value_props rvp
    cross join jsonb_array_elements_text(
        case 
            when rvp.r_value_propositions is null then '[]'::jsonb
            when jsonb_typeof(rvp.r_value_propositions) = 'array' then rvp.r_value_propositions
            else '[]'::jsonb
        end
    ) as value_prop_text
    where value_prop_text is not null 
      and trim(value_prop_text) != ''
      and trim(value_prop_text) != 'null'
),

-- Clean and standardize value propositions
final as (
    select
        review_id,
        ad_account_id,
        review_date,
        review_author,
        lower(trim(value_proposition)) as value_proposition,
        importance_score::numeric as importance_score,
        lower(trim(sentiment)) as sentiment,
        'review' as source_system,
        loaded_at
    from normalized_value_props
    where value_proposition is not null
      and value_proposition != ''
      and value_proposition != 'null'
)

select * from final 