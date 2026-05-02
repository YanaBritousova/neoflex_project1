create or replace procedure dm.fill_f101_round_f(i_OnDate date) 
language plpgsql as $$
declare
from_date_var date;
to_date_var date;
prev_date date;
begin
from_date_var := i_OnDate - interval '1 month';
to_date_var := i_OnDate - interval '1 day';
prev_date := from_date_var - interval '1 day';

delete from dm.dm_f101_round_f where from_date = from_date_var and to_date = to_date_var;

insert into DM.DM_F101_ROUND_F (from_date, to_date, chapter, ledger_account, characteristic,
balance_in_rub, balance_in_val, balance_in_total, turn_deb_rub, turn_deb_val, turn_deb_total,
turn_cre_rub, turn_cre_val, turn_cre_total, balance_out_rub, balance_out_val, balance_out_total
)
select from_date_var, to_date_var , la.chapter, la.ledger_account::varchar, a.char_type as characteristic,

sum (case when a.currency_code in ('810', '643') then coalesce(ab.balance_out_rub,0)
else 0 end)as balance_in_rub,
sum(case when a.currency_code not in ('810', '643') then coalesce(ab.balance_out_rub,0)
else 0 end) as balance_in_val,
sum(coalesce(ab.balance_out_rub,0)) as balance_in_total,

sum (case when a.currency_code in ('810', '643') then coalesce(dat.debet_amount_rub,0)
else 0 end) as turn_deb_rub,
sum (case when a.currency_code not in ('810', '643') then coalesce(dat.debet_amount_rub,0)
else 0 end) as turn_deb_val,
sum(coalesce(dat.debet_amount_rub,0)) as turn_deb_total,

sum (case when a.currency_code in ('810', '643') then coalesce(dat.credit_amount_rub,0)
else 0 end) as turn_cre_rub,
sum (case when a.currency_code not in ('810', '643') then coalesce(dat.credit_amount_rub,0)
else 0 end) as turn_cre_val,
sum(coalesce(dat.credit_amount_rub,0)) as turn_cre_total,

sum (case when a.currency_code in ('810', '643') then coalesce(ab2.balance_out_rub,0)
else 0 end) as balance_out_rub,
sum (case when a.currency_code not in ('810', '643') then coalesce(ab2.balance_out_rub,0)
else 0 end) as balance_out_val,
sum(coalesce(ab2.balance_out_rub,0)) as balance_out_total

from ds.md_account_d a left join ds.md_ledger_account_s la 
on substring(a.account_number,1, 5) = la.ledger_account::text
left join dm.dm_account_balance_f ab on ab.account_rk = a.account_rk and ab.on_date=prev_date 
left join dm.dm_account_turnover_f dat on dat.account_rk = a.account_rk and dat.on_date between from_date_var and to_date_var
left join dm.dm_account_balance_f ab2 on ab.account_rk = a.account_rk and ab2.on_date=to_date_var
group by from_date_var, to_date_var , la.chapter, la.ledger_account, a.char_type;
end; $$;