with orders as (
    select * from {{ ref('stg_orders') }}
),

percentages as (
    select
        Name,
        TotalKg,
        Best3SquatKg,
        Best3BenchKg,
        Best3DeadliftKg,
        ROUND(Best3SquatKg / TotalKg, 2) as SquatPercentage,
        ROUND(Best3Benchkg / TotalKg, 2) as BenchPercentage,
        ROUND(Best3DeadliftKg / TotalKg, 2) as DeadliftPercentage 
    from orders
),

final as (
    select
        orders.*,
        percentages.SquatPercentage,
        percentages.BenchPercentage,
        percentages.DeadliftPercentage
    from orders
    join percentages using (name, TotalKg)
)

select * from final