create or replace procedure ds.fill_account_balance_f(i_OnDate date) 
language plpgsql as $$
declare
rate numeric;
begin
delete from dm.dm_account_balance_f where on_date=i_OnDate;
select reduced_cource into rate from ds.md_exchange_rate_d
where i_OnDate between data_actual_date and data_actual_end_date limit 1;
rate:=coalesce(rate,1);

insert into dm.dm_account_balance_f
select t.on_date, a.account_rk, 
case
when a.char_type='А' then coalesce(b.balance_out,0) +coalesce(t.debet_amount,0)-coalesce(t.credit_amount,0)
when a.char_type='П' then coalesce(b.balance_out,0) -coalesce(t.debet_amount,0)+coalesce(t.credit_amount,0)
end
as balance_out,
case
when a.char_type='А' then 
(coalesce(b.balance_out,0)+coalesce(t.debet_amount,0)-coalesce(t.credit_amount,0))*rate
when a.char_type='П' then 
(coalesce(b.balance_out,0)-coalesce(t.debet_amount,0)+coalesce(t.credit_amount,0))*rate
end
as balance_out_rub
from ds.md_account_d a
left join ds.ft_balance_f b on a.account_rk=b.account_rk
and b.on_date = i_OnDate - 1
join dm.dm_account_turnover_f t on a.account_rk=t.account_rk
and t.on_date=i_OnDate
where i_OnDate between a.data_actual_date and a.data_actual_end_date;

end; $$;