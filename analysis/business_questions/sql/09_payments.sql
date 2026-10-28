-- Платёж — строка активного fct_payment; все статусы, без соединения с позициями.
-- Штат и год — по заказу. Все годы в витрине, здесь срез 2018 (27 штатов).
-- Оба полных разреза: 103 886 платежей, 16 008 872.12 BRL, сумма рассрочек 296 450.
-- not_defined сохраняется: 3 платежа на 0.00; рассрочка 0 у 2 credit_card учитывается как есть.
select customer_state, purchase_year, payments_count, payment_amount, installments_sum,
    round(avg_installments, 4) as avg_installments, zero_installments_count
from mart.payments_by_state_year
where purchase_year = 2018
order by payment_amount desc, customer_state;

select payment_type, payments_count, payment_amount, installments_sum,
    round(avg_installments, 4) as avg_installments, zero_installments_count
from mart.payments_by_type
order by payment_amount desc, payment_type;
