/*
Examples for RLS/CLS/TLS usage with customer A.
*/

-- 1) Optional test table
IF OBJECT_ID('sales.Orders', 'U') IS NULL
BEGIN
    EXEC('CREATE SCHEMA sales');
    CREATE TABLE sales.Orders (
        OrderID INT IDENTITY(1,1) PRIMARY KEY,
        CustomerID NVARCHAR(50) NOT NULL,
        Amount DECIMAL(18,2) NOT NULL,
        Salary DECIMAL(18,2) NULL,
        Region NVARCHAR(50) NULL
    );
END;
GO

-- 2) Security policy for row filtering
IF NOT EXISTS (SELECT 1 FROM sys.security_policies WHERE name = 'Policy_sales_Orders')
BEGIN
    EXEC('
        CREATE SECURITY POLICY security.[Policy_sales_Orders]
        ADD FILTER PREDICATE security.fn_rls_customer_filter(CustomerID)
        ON [sales].[Orders]
        WITH (STATE = ON);
    ');
END;
GO

-- 3) Role per customer for CLS/TLS
IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = 'cust_A' AND type = 'R')
BEGIN
    EXEC('CREATE ROLE [cust_A]');
END;
GO

-- TLS: allow table
GRANT SELECT ON [sales].[Orders] TO [cust_A];
GO

-- CLS: hide sensitive column Salary
DENY SELECT ON OBJECT::[sales].[Orders] ([Salary]) TO [cust_A];
GO

-- 4) Identity map (oid primary, upn optional)
DELETE FROM security.CustomerIdentityMap WHERE CustomerID = 'A';
INSERT INTO security.CustomerIdentityMap (IdentityOid, IdentityUpn, CustomerID, IsActive, CreatedAt, UpdatedAt)
VALUES
('00000000-0000-0000-0000-000000000001', 'customer.a@contoso.com', 'A', 1, SYSUTCDATETIME(), SYSUTCDATETIME());
GO

-- 5) Metadata record
MERGE security.CustomerPolicies AS tgt
USING (SELECT 'A' AS CustomerID, 'sales' AS SchemaName, 'Orders' AS TableName) AS src
ON tgt.CustomerID = src.CustomerID
AND tgt.SchemaName = src.SchemaName
AND tgt.TableName = src.TableName
WHEN MATCHED THEN
    UPDATE SET
        AllowedColumns = 'OrderID,CustomerID,Amount,Region',
        FilterColumn = 'CustomerID',
        FilterOperator = '=',
        FilterValue = 'A',
        TableAccess = 1,
        UpdatedAt = SYSUTCDATETIME()
WHEN NOT MATCHED THEN
    INSERT (CustomerID, SchemaName, TableName, AllowedColumns, FilterColumn, FilterOperator, FilterValue, TableAccess, UpdatedAt)
    VALUES ('A', 'sales', 'Orders', 'OrderID,CustomerID,Amount,Region', 'CustomerID', '=', 'A', 1, SYSUTCDATETIME());
GO
