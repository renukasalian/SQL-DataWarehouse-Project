/*
==================================================================================
Create Database and Schemas
==================================================================================
Script Purpose:
  This script creates a 'DataWarehouse' database.
  Before creating it, the script checks if a database with the same name already exists.
  If it does, the existing one is deleted and then created again.
  It also sets up three schemas inside the database: 'gold' , 'silver' and 'bronze'.

WARNING:
  Running this script will erase the entire 'DataWarehouse' database if exists.
  All the data in  the db will be permanently deleted.
  Proceed mindfully and ensure you have proper backups before running the scripts.
*/


USE master;
GO

--DROP RECREATE THE 'DataWarehouse' DATABASE IF IT ALREADY EXISTS, IF NOT RECREATE
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	
	DROP DATABASE DataWarehouse;
END;
GO

--Create the 'DataWarehouse' database 
CREATE DATABASE DataWarehouse;

USE DataWarehouse;

CREATE SCHEMA bronze;

CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
