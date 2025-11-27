/*
=============================================================================================================
DDL : CREATE BRONZE LAYER
=============================================================================================================
Script Purpose:
  This script recreates all tables under the 'bronze' schema.
  Any existing tables will be removed first to ensure a clean rebuild.
Execute this script whenever you need to reinitialize the bronze layer DDL.
*/

-- Before creating table checking if it already exists. 
USE DataWarehouse;

IF OBJECT_ID ('bronze.crm_cust_info','U') IS NOT NULL
	DROP TABLE bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info(
	cst_id INT,
	cst_key VARCHAR(40),
	cst_firstname VARCHAR(40),
	cst_lastname VARCHAR(40),
	cst_marital_status VARCHAR(10),
	cst_gndr VARCHAR(10),
	cst_create_date DATE
);

GO
IF OBJECT_ID ('bronze.crm_prd_cust_info','U') IS NOT NULL
	DROP TABLE bronze.crm_prd_cust_info;
CREATE TABLE bronze.crm_prd_cust_info(
	prd_id INT,
	prd_key VARCHAR(50),
	prd_nm VARCHAR(50),
	prd_cost INT,
	prd_line VARCHAR(40),
	prd_start_dt DATETIME,
	prd_end_dt DATETIME


);

GO
IF OBJECT_ID ('bronze.crm_sales_details','U') IS NOT NULL
	DROP TABLE bronze.crm_sales_details;

CREATE TABLE bronze.crm_sales_details(
	sls_ord_num VARCHAR(30),
	sls_prd_key VARCHAR(30),
	sls_cust_id INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT
);

GO
IF OBJECT_ID ('bronze.erp_cust_az12','U') IS NOT NULL
	DROP TABLE bronze.erp_cust_az12;

CREATE TABLE bronze.erp_cust_az12(
	cid VARCHAR(40),
	bdate DATE,
	gen VARCHAR(15)
);

GO
IF OBJECT_ID ('bronze.erp_px_cat_g1v2','U') IS NOT NULL
	DROP TABLE bronze.erp_px_cat_g1v2;

CREATE TABLE bronze.erp_px_cat_g1v2(
	id VARCHAR(20),
	cat VARCHAR(30),
	subcat VARCHAR(30),
	maintenance VARCHAR(15)
);

GO
IF OBJECT_ID ('bronze.erp_loc_a101','U') IS NOT NULL
	DROP TABLE bronze.erp_loc_a101;

CREATE TABLE bronze.erp_loc_a101(
	cid VARCHAR(30),
	cntry VARCHAR(20)
);


