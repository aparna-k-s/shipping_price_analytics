{{config(
    tags=["credit_valuation"]
)}}

{#WITH credit AS (#}
{#    SELECT#}
{#        credit_value#}
{#        , exchange_ledger.current_credit_valuation(record_type, year_month, subcategory) as expected_value#}
{#    FROM {{ ref('credit_valuation_matrix') }}#}
{#    ORDER BY RANDOM()#}
{#    LIMIT 1000#}
{#)#}
{#SELECT *#}
{#FROM credit#}
{#WHERE credit_value != expected_value#}

    select 1