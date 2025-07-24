-- models/intermediate/int_review_barriers.sql
-- This model normalizes JSON barriers from customer reviews into structured rows

{{ config(
    materialized='table',
    indexes=[
        {'columns': ['ad_account_id'], 'type': 'btree'},
        {'columns': ['barrier'], 'type': 'btree'},
        {'columns': ['review_date'], 'type': 'btree'}
    ]
) }}

with review_barriers as (
    select
        review_id,
        ad_account_id,
        review_date,
        review_author,
        r_barriers,
        loaded_at
    from {{ ref('stg_review__reviews') }}
    where r_barriers is not null
),

-- Extract barriers from JSON array
normalized_barriers as (
    select
        review_id,
        ad_account_id,
        review_date,
        review_author,
        barrier_text as barrier,
        '3' as impact_score,  -- Default impact
        'general' as barrier_type,  -- Default type
        loaded_at
    from review_barriers rb
    cross join jsonb_array_elements_text(
        case 
            when rb.r_barriers is null then '[]'::jsonb
            when jsonb_typeof(rb.r_barriers) = 'array' then rb.r_barriers
            else '[]'::jsonb
        end
    ) as barrier_text
    where barrier_text is not null 
      and trim(barrier_text) != ''
      and trim(barrier_text) != 'null'
),

-- Clean and standardize barriers
final as (
    select
        review_id,
        ad_account_id,
        review_date,
        review_author,
        lower(trim(barrier)) as barrier,
        case 
            when impact_score ~ '^[0-9]+\.?[0-9]*$' 
            then impact_score::numeric 
            else null 
        end as impact_score,
        lower(trim(barrier_type)) as barrier_type,
        'review' as source_system,
        loaded_at
    from normalized_barriers
    where barrier is not null
      and barrier != ''
      and barrier != 'null'
)

select * from final 