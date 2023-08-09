-- Databricks notebook source
CREATE OR REPLACE TABLE expense_report.apple_card.monthly_credit_card_payment
(
  credit_payment_date TIMESTAMP,
  credit_payment_description STRING,
  credit_payment_amount_usd DECIMAL(10,2)
);

CREATE OR REPLACE TABLE expense_report.apple_card.monthly_credit_transaction
(
  trans_date TIMESTAMP,
  trans_store_desc STRING ,
  trans_store STRING,
  trans_cash_perc DECIMAL(14, 6), 
  trans_cashback_amount DECIMAL(10, 2),
  trans_cash_amount DECIMAL(10,2)
);

CREATE OR REPLACE TABLE expense_report.apple_card.monthly_apple_installment_transaction
(
  transaction_date TIMESTAMP,
  apple_installment_loc STRING ,
  transaction_id STRING,
  transaction_amount DECIMAL(10, 2)
);

CREATE OR REPLACE TABLE expense_report.apple_card.shopper_dim_table
(
  store_name_raw STRING,
  store_name STRING,
  store_category STRING,
  store_addr STRING,
  store_contact_number STRING
);
