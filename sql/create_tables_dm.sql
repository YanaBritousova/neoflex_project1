create schema dm;

create table dm.dm_account_turnover_f (
    on_date date,
    account_rk numeric,
    credit_amount numeric(23,8),
    credit_amount_rub numeric(23,8),
    debet_amount numeric(23,8),
    debet_amount_rub numeric(23,8)
);

create table dm.dm_account_balance_f (
    on_date date,
    account_rk numeric,
    balance_out numeric(23,8),
    balance_out_rub numeric(23,8)
);