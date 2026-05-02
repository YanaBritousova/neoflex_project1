create or replace procedure ds.fill_account_turnover_f(i_OnDate timestamp) 
language plpgsql as $$
declare
rate numeric;
begin

delete from dm.dm_account_turnover_f where on_date=i_OnDate;

select reduced_cource into rate from ds.md_exchange_rate_d
where i_OnDate between data_actual_date and data_actual_end_date limit 1;

rate:=coalesce(rate, 1);

insert into dm.dm_account_turnover_f (on_date, account_rk, credit_amount, credit_amount_rub, debet_amount, debet_amount_rub)
select coalesce (c.oper_date, d.oper_date),
coalesce (c.account_rk, d.account_rk),
credit_amount, credit_amount_rub, debet_amount, debet_amount_rub
from 
(select oper_date, 
credit_account_rk as account_rk, 
sum(credit_amount) as credit_amount,
sum(credit_amount) * rate as credit_amount_rub
from ds.ft_posting_f 
where oper_date = i_OnDate
group by oper_date, credit_account_rk) c 
full outer join 
(select oper_date, 
debet_account_rk as account_rk, 
sum(debet_amount) as debet_amount,
sum(debet_amount) * rate as debet_amount_rub
from ds.ft_posting_f 
where oper_date = i_OnDate
group by oper_date, debet_account_rk) d 
on c.account_rk=d.account_rk;
end; $$;