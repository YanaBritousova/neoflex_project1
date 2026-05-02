create schema if not exists dm;

create table if not exists dm.dm_account_turnover_f (
    on_date date,
    account_rk numeric,
    credit_amount numeric(23,8),
    credit_amount_rub numeric(23,8),
    debet_amount numeric(23,8),
    debet_amount_rub numeric(23,8)
);

create table if not exists dm.dm_account_balance_f (
    on_date date,
    account_rk numeric,
    balance_out numeric(23,8),
    balance_out_rub numeric(23,8)
);

create table if not exists dm.dm_f101_round_f (
    from_date date,
    to_date date,
    chapter char(1),
    ledger_account char(5),
    characteristic char(1),
    balance_in_rub numeric(23,8),
    balance_in_val numeric(23,8),
    balance_in_total numeric(23,8),
    turn_deb_rub numeric(23,8),
    turn_deb_val numeric(23,8),
    turn_deb_total numeric(23,8),
    turn_cre_rub numeric(23,8),
    turn_cre_val numeric(23,8),
    turn_cre_total numeric(23,8),
    balance_out_rub numeric(23,8),
    balance_out_val numeric(23,8),
    balance_out_total numeric(23,8)
);