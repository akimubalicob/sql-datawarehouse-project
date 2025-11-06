-- ================================================================
-- ETL Script: Bronze to Silver Layer Transformation
-- Author: Serdi Akbar

-- Purpose:
-- This script performs a full refresh of selected silver-layer tables
-- by truncating existing data and inserting cleaned, standardized records
-- from the bronze layer. It ensures data quality and consistency for downstream use.

-- Enhancements:
-- - Logs each step with PRINT statements
-- - Measures duration per section and total runtime
-- - Implements TRY...CATCH for error handling

-- Usage
-- EXEC silver.load_silver;

-- ================================================================

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	SET NOCOUNT ON;

	DECLARE @start_time DATETIME = GETDATE();
	DECLARE @step_start DATETIME;
	DECLARE @step_end DATETIME;
	DECLARE @step_duration VARCHAR(50);

	BEGIN TRY

		-- crm_cust_info
		SET @step_start = GETDATE();
		PRINT '>> Truncating Table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;

		PRINT '>> Inserting Data Into: silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info (
			cst_id, cst_key, cst_firstname, cst_lastname,
			cst_marital_status, cst_gndr, cst_create_date
		)
		SELECT 
			cst_id,
			cst_key,
			TRIM(cst_firstname),
			TRIM(cst_lastname),
			CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				 ELSE 'n/a' END,
			CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				 ELSE 'n/a' END,
			cst_create_date
		FROM (
			SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		) t
		WHERE flag_last = 1;

		SET @step_end = GETDATE();
		SET @step_duration = CAST(DATEDIFF(SECOND, @step_start, @step_end) AS VARCHAR) + ' seconds';
		PRINT '>> crm_cust_info completed in ' + @step_duration;
		PRINT '================================================='

		-- crm_prd_info
		SET @step_start = GETDATE();
		PRINT '>> Truncating Table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;

		PRINT '>> Inserting Data Into: silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info (
			prd_id, cat_id, prd_key, prd_nm,
			prd_cost, prd_line, prd_start_dt, prd_end_dt
		)
		SELECT
			prd_id,
			REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_'),
			SUBSTRING(prd_key, 7, LEN(prd_key)),
			prd_nm,
			COALESCE(prd_cost, 0),
			CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
				 WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
				 WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
				 WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'other Sales'
				 ELSE 'n/a' END,
			CAST(prd_start_dt AS DATE),
			CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt ASC) - 1 AS DATE)
		FROM bronze.crm_prd_info;

		SET @step_end = GETDATE();
		SET @step_duration = CAST(DATEDIFF(SECOND, @step_start, @step_end) AS VARCHAR) + ' seconds';
		PRINT '>> crm_prd_info completed in ' + @step_duration;
		PRINT '================================================='

		-- crm_sales_details
		SET @step_start = GETDATE();
		PRINT '>> Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;

		PRINT '>> Inserting Data Into: silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details (
			sls_ord_num, sls_prd_key, sls_cust_id,
			sls_order_dt, sls_ship_dt, sls_due_dt,
			sls_sales, sls_quantity, sls_price
		)
		SELECT
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
				 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) END,
			CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) END,
			CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
				 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) END,
			CASE WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price)
				 THEN sls_quantity * sls_price ELSE sls_sales END,
			sls_quantity,
			CASE WHEN sls_price IS NULL OR sls_price <= 0
				 THEN sls_sales / NULLIF(sls_quantity, 0) ELSE sls_price END
		FROM bronze.crm_sales_details;

		SET @step_end = GETDATE();
		SET @step_duration = CAST(DATEDIFF(SECOND, @step_start, @step_end) AS VARCHAR) + ' seconds';
		PRINT '>> crm_sales_details completed in ' + @step_duration;
		PRINT '================================================='

		-- erp_px_cat_g1v2
		SET @step_start = GETDATE();
		PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;

		PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2 (
			id, cat, subcat, maintenance
		)
		SELECT id, cat, subcat, maintenance
		FROM bronze.erp_px_cat_g1v2;

		SET @step_end = GETDATE();
		SET @step_duration = CAST(DATEDIFF(SECOND, @step_start, @step_end) AS VARCHAR) + ' seconds';
		PRINT '>> erp_px_cat_g1v2 completed in ' + @step_duration;
		PRINT '================================================='

		-- erp_cust_az12
		SET @step_start = GETDATE();
		PRINT '>> Truncating Table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;

		PRINT '>> Inserting Data Into: silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12 (
			cid, bdate, gen
		)
		SELECT
			CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) ELSE cid END,
			CASE WHEN bdate > GETDATE() THEN NULL ELSE bdate END,
			CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
				 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
				 ELSE 'n/a' END
		FROM bronze.erp_cust_az12;

		SET @step_end = GETDATE();
		SET @step_duration = CAST(DATEDIFF(SECOND, @step_start, @step_end) AS VARCHAR) + ' seconds';
		PRINT '>> erp_cust_az12 completed in ' + @step_duration;
		PRINT '================================================='

		-- erp_loc_a101
		SET @step_start = GETDATE();
		PRINT '>> Truncating Table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;

		PRINT '>> Inserting Data Into: silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101 (
			cid, cntry
		)
		SELECT
			REPLACE(cid, '-', ''),
			CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
				 WHEN UPPER(TRIM(cntry)) IN ('US', 'USA') THEN 'United States'
				 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
				 ELSE TRIM(cntry) END
		FROM bronze.erp_loc_a101;

		SET @step_end = GETDATE();
		SET @step_duration = CAST(DATEDIFF(SECOND, @step_start, @step_end) AS VARCHAR) + ' seconds';
		PRINT '>> erp_loc_a101 completed in ' + @step_duration;
		PRINT '================================================='

		-- Total duration
		DECLARE @total_duration VARCHAR(50) = CAST(DATEDIFF(SECOND, @start_time, GETDATE()) AS VARCHAR) + ' seconds';
		PRINT '>> Silver layer loading completed in ' + @total_duration;

	END TRY
	BEGIN CATCH
		DECLARE @err_msg NVARCHAR(MAX) = ERROR_MESSAGE();
		DECLARE @err_line INT = ERROR_LINE();
		DECLARE @err_proc SYSNAME = ERROR_PROCEDURE();
		PRINT '>> ERROR in procedure [' + ISNULL(@err_proc, 'unknown') + '] at line ' + CAST(@err_line AS VARCHAR);
		PRINT '>> Message: ' + @err_msg;
	END CATCH
END
