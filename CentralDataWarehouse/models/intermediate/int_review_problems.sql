-- models/intermediate/int_review_problems.sql
-- This model normalizes JSON problems from customer reviews into structured rows

{{ config(
    materialized='table',
    indexes=[
        {'columns': ['ad_account_id'], 'type': 'btree'},
        {'columns': ['problem'], 'type': 'btree'},
        {'columns': ['review_date'], 'type': 'btree'}
    ]
) }}

with review_problems as (
    select
        review_id,
        ad_account_id,
        review_date,
        review_author,
        r_problems,
        loaded_at
    from {{ ref('stg_review__reviews') }}
    where r_problems is not null
),

-- Extract problems from JSON array
normalized_problems as (
    select
        review_id,
        ad_account_id,
        review_date,
        review_author,
        trim(json_extract_path_text(problem.value, 'problem')) as problem,
        trim(json_extract_path_text(problem.value, 'severity')) as severity_score,
        trim(json_extract_path_text(problem.value, 'category')) as problem_category,
        loaded_at
    from review_problems rp
    cross join json_array_elements(rp.r_problems) as problem
    where trim(json_extract_path_text(problem.value, 'problem')) != ''
),

-- Clean and standardize problems
final as (
    select
        review_id,
        ad_account_id,
        review_date,
        review_author,
        lower(trim(problem)) as problem,
        case 
            when severity_score ~ '^[0-9]+\.?[0-9]*$' 
            then severity_score::numeric 
            else null 
        end as severity_score,
        lower(trim(problem_category)) as problem_category,
        'review' as source_system,
        loaded_at
    from normalized_problems
    where problem is not null
      and problem != ''
      and problem != 'null'
)

select * from final 