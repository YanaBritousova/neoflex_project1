create schema if not exists ds;

create table if not exists ds.ft_balance_f (
    on_date date not null,
    account_rk int not null,
    currency_rk int,
    balance_out float8,
    primary key (on_date, account_rk)
);

create table if not exists ds.ft_posting_f (
    oper_date date not null,
    credit_account_rk int not null,
    debet_account_rk int not null,
    credit_amount float8,
    debet_amount float8
);

create table if not exists ds.md_account_d (
    data_actual_date date not null,
    data_actual_end_date date not null,
    account_rk int not null,
    account_number varchar(20) not null,
    char_type varchar(1) not null,
    currency_rk int not null,
    currency_code varchar(3) not null,
    primary key (data_actual_date, account_rk)
);

create table if not exists ds.md_currency_d (
    currency_rk int not null,
    data_actual_date date not null,
    data_actual_end_date date,
    currency_code varchar(3),
    code_iso_char varchar(3),
    primary key (currency_rk, data_actual_date)
);

create table if not exists ds.md_exchange_rate_d (
    data_actual_date date not null,
    data_actual_end_date date,
    currency_rk int not null,
    reduced_cource float8,
    code_iso_num varchar(3),
    primary key (data_actual_date, currency_rk)
);

create table if not exists ds.md_ledger_account_s (
    chapter char(1),
    chapter_name varchar(16),
    section_number int,
    section_name varchar(22),
    subsection_name varchar(21),
    ledger1_account int,
    ledger1_account_name varchar(47),
    ledger_account int not null,
    ledger_account_name varchar(153),
    characteristic char(1),
    is_resident int,
    is_reserve int,
    is_reserved int,
    is_loan int,
    is_reserved_assets int,
    is_overdue int,
    is_interest int,
    pair_account varchar(5),
    start_date date not null,
    end_date date,
    is_rub_only int,
    min_term varchar(1),
    min_term_measure varchar(1),
    max_term varchar(1),
    max_term_measure varchar(1),
    ledger_acc_full_name_translit varchar(1),
    is_revaluation varchar(1),
    is_correct varchar(1),
    primary key (ledger_account, start_date)
);

create schema if not exists logs;

create table logs.logs(
	log_id serial primary key,
	affected_table_name varchar(50),
	start_timestamp timestamp not null,
	end_timestamp timestamp,
	duration numeric(10,2),
	status varchar(20),
	error_message text
);