-- Databricks notebook source
CREATE OR REPLACE TABLE expense_report.apple_card.monthly_credit_transaction_cleaned AS 
(
  SELECT 
    * 
  FROM 
    expense_report.apple_card.monthly_credit_transaction as MCT
  LEFT JOIN 
    expense_report.apple_card.shopper_dim_table as SDT ON MCT.trans_store	 = SDT.store_name_raw
);
