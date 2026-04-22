/*
Bootstrap script for Fabric SQL security metadata and reusable RLS function.
Run this once before calling the Azure Function APIs.
*/

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'security')
    EXEC('CREATE SCHEMA security');
GO

IF OBJECT_ID('security.CustomerIdentityMap', 'U') IS NULL
BEGIN
    CREATE TABLE security.CustomerIdentityMap (
        IdentityOid NVARCHAR(128) NOT NULL,
        IdentityUpn NVARCHAR(256) NULL,
        CustomerID NVARCHAR(128) NOT NULL,
        IsActive BIT NOT NULL DEFAULT (1),
        CreatedAt DATETIME2 NOT NULL DEFAULT (SYSUTCDATETIME()),
        UpdatedAt DATETIME2 NOT NULL DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_CustomerIdentityMap PRIMARY KEY (IdentityOid, CustomerID)
    );
END
GO

IF OBJECT_ID('security.CustomerPolicies', 'U') IS NULL
BEGIN
    CREATE TABLE security.CustomerPolicies (
        CustomerID NVARCHAR(128) NOT NULL,
        SchemaName NVARCHAR(128) NOT NULL,
        TableName NVARCHAR(128) NOT NULL,
        AllowedColumns NVARCHAR(MAX) NULL,
        FilterColumn NVARCHAR(128) NULL,
        FilterOperator NVARCHAR(10) NULL,
        FilterValue NVARCHAR(256) NULL,
        TableAccess BIT NOT NULL DEFAULT (1),
        UpdatedAt DATETIME2 NOT NULL DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_CustomerPolicies PRIMARY KEY (CustomerID, SchemaName, TableName)
    );
END
GO

CREATE OR ALTER FUNCTION security.fn_rls_customer_filter (@RowCustomerId NVARCHAR(128))
RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN
(
    SELECT
        1 AS fn_result
    WHERE EXISTS
    (
        SELECT
            1
        FROM security.CustomerIdentityMap map
        WHERE map.IsActive = 1
          AND map.CustomerID = @RowCustomerId
          AND
          (
              map.IdentityOid = CAST(SESSION_CONTEXT(N'customer_oid') AS NVARCHAR(128))
              OR (
                    map.IdentityUpn IS NOT NULL
                    AND map.IdentityUpn = CAST(SESSION_CONTEXT(N'customer_upn') AS NVARCHAR(256))
                 )
          )
    )
);
GO

/*
Example policy (table specific):
CREATE SECURITY POLICY security.Policy_sales_Orders
ADD FILTER PREDICATE security.fn_rls_customer_filter(CustomerID)
ON [sales].[Orders]
WITH (STATE = ON);
GO
*/
