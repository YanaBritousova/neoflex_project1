insert into dm.dm_account_balance_f (on_date, account_rk, balance_out, balance_out_rub)
select 
    fb.on_date,
    fb.account_rk,
    fb.balance_out,
    fb.balance_out * coalesce(er.reduced_cource, 1) as balance_out_rub
from ds.ft_balance_f fb
left join ds.md_exchange_rate_d er 
    on er.currency_rk = fb.currency_rk
    and er.data_actual_date <= fb.on_date
    and (er.data_actual_end_date is null or er.data_actual_end_date >= fb.on_date)
where fb.on_date = '2017-12-31';