{{ config(severity='error') }}
with expected as (
    select coalesce(sum(payment_value), 0) as amount, count(*) as payments,
        coalesce(sum(payment_installments), 0) as installments,
        count(*) filter (where payment_installments = 0) as zero_count
    from {{ ref('fct_payment__active') }}
), actual as (
    select 'state_year' as grain, coalesce(sum(payment_amount), 0) as amount,
        coalesce(sum(payments_count), 0) as payments, coalesce(sum(installments_sum), 0) as installments,
        coalesce(sum(zero_installments_count), 0) as zero_count
    from {{ ref('payments_by_state_year') }}
    union all
    select 'type', coalesce(sum(payment_amount), 0), coalesce(sum(payments_count), 0),
        coalesce(sum(installments_sum), 0), coalesce(sum(zero_installments_count), 0)
    from {{ ref('payments_by_type') }}
)
select a.* from actual a cross join expected e
where a.amount is distinct from e.amount or a.payments <> e.payments
    or a.installments <> e.installments or a.zero_count <> e.zero_count
