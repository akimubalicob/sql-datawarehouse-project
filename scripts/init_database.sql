-- ================================================
-- Script: Initialize 'DataWarehouse' Database
-- Purpose: Create a new database with bronze, silver, and gold schemas
-- ================================================

-- Step 1: Create the database
USE master;
GO

CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- ================================================
-- Step 2: Drop and Recreate Schemas
-- ================================================

-- Drop and recreate 'bronze' schema
IF EXISTS (SELECT * FROM sys.schemas WHERE name = 'bronze')
    DROP SCHEMA bronze;
GO

CREATE SCHEMA bronze;
GO

-- Drop and recreate 'silver' schema
IF EXISTS (SELECT * FROM sys.schemas WHERE name = 'silver')
    DROP SCHEMA silver;
GO

CREATE SCHEMA silver;
GO

-- Drop and recreate 'gold' schema
IF EXISTS (SELECT * FROM sys.schemas WHERE name = 'gold')
    DROP SCHEMA gold;
GO

CREATE SCHEMA gold;
GO
