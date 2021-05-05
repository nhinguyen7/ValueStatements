
-- Table: lcdm_main.customer_account_type_model_score

-- ALTER TABLE lcdm_main.customer_account_type_model_score - 2 new cols 20200504;
ALTER TABLE lcdm_main.customer_account_type_model_score
ADD 
	total_savings numeric(12,7),
	total_savings_ty numeric(12,7)
;