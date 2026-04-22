from __future__ import annotations

import re
from typing import Any

from src.models import PolicyRequest


IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
ALLOWED_OPERATORS = {"=", "<>", "!=", ">", "<", ">=", "<="}


def validate_identifier(name: str, label: str) -> None:
    if not name or not IDENTIFIER_RE.match(name):
        raise ValueError(f"{label} has invalid characters.")


def quote_identifier(name: str) -> str:
    validate_identifier(name, "Identifier")
    return f"[{name}]"


def validate_policy_request(req: PolicyRequest) -> None:
    validate_identifier(req.schema_name, "schemaName")
    validate_identifier(req.table_name, "tableName")
    for col in req.allowed_columns:
        validate_identifier(col, "allowedColumns value")
    if req.row_filter:
        validate_identifier(req.row_filter.column, "rowFilter.column")
        if req.row_filter.operator not in ALLOWED_OPERATORS:
            raise ValueError(
                f"rowFilter.operator must be one of: {', '.join(sorted(ALLOWED_OPERATORS))}"
            )
    for identity in req.identities:
        if not identity.oid.strip():
            raise ValueError("identities.oid cannot be empty.")


def ensure_table_and_columns_sql(req: PolicyRequest) -> list[tuple[str, list[Any]]]:
    return [
        (
            """
            SELECT 1
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
            """,
            [req.schema_name, req.table_name],
        ),
        (
            """
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
            """,
            [req.schema_name, req.table_name],
        ),
    ]


def bootstrap_rls_sql() -> list[tuple[str, list[Any]]]:
    return [
        (
            """
            IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'security')
                EXEC('CREATE SCHEMA security');
            """,
            [],
        ),
        (
            """
            IF OBJECT_ID('security.CustomerIdentityMap', 'U') IS NULL
            CREATE TABLE security.CustomerIdentityMap (
                IdentityOid NVARCHAR(128) NOT NULL,
                IdentityUpn NVARCHAR(256) NULL,
                CustomerID NVARCHAR(128) NOT NULL,
                IsActive BIT NOT NULL DEFAULT(1),
                CreatedAt DATETIME2 NOT NULL DEFAULT(SYSUTCDATETIME()),
                UpdatedAt DATETIME2 NOT NULL DEFAULT(SYSUTCDATETIME()),
                CONSTRAINT PK_CustomerIdentityMap PRIMARY KEY (IdentityOid, CustomerID)
            );
            """,
            [],
        ),
        (
            """
            IF OBJECT_ID('security.CustomerPolicies', 'U') IS NULL
            CREATE TABLE security.CustomerPolicies (
                CustomerID NVARCHAR(128) NOT NULL,
                SchemaName NVARCHAR(128) NOT NULL,
                TableName NVARCHAR(128) NOT NULL,
                AllowedColumns NVARCHAR(MAX) NULL,
                FilterColumn NVARCHAR(128) NULL,
                FilterOperator NVARCHAR(10) NULL,
                FilterValue NVARCHAR(256) NULL,
                TableAccess BIT NOT NULL DEFAULT(1),
                UpdatedAt DATETIME2 NOT NULL DEFAULT(SYSUTCDATETIME()),
                CONSTRAINT PK_CustomerPolicies PRIMARY KEY (CustomerID, SchemaName, TableName)
            );
            """,
            [],
        ),
        (
            """
            IF OBJECT_ID('security.fn_rls_customer_filter', 'IF') IS NULL
                EXEC('
                    CREATE FUNCTION security.fn_rls_customer_filter(@RowCustomerId NVARCHAR(128))
                    RETURNS TABLE
                    WITH SCHEMABINDING
                    AS
                    RETURN (
                        SELECT 1 AS fn_result
                        WHERE EXISTS (
                            SELECT 1
                            FROM security.CustomerIdentityMap m
                            WHERE m.IsActive = 1
                              AND m.CustomerID = @RowCustomerId
                              AND (
                                  m.IdentityOid = CAST(SESSION_CONTEXT(N''customer_oid'') AS NVARCHAR(128))
                                  OR (
                                      m.IdentityUpn IS NOT NULL
                                      AND m.IdentityUpn = CAST(SESSION_CONTEXT(N''customer_upn'') AS NVARCHAR(256))
                                  )
                              )
                        )
                    )
                ');
            """,
            [],
        ),
    ]


def build_upsert_policy_sql(
    req: PolicyRequest,
    existing_columns: list[str],
) -> list[tuple[str, list[Any]]]:
    validate_policy_request(req)
    role_name = f"cust_{req.customer_id}".replace("-", "_")
    validate_identifier(role_name, "Derived role name")

    schema_q = quote_identifier(req.schema_name)
    table_q = quote_identifier(req.table_name)
    role_q = quote_identifier(role_name)
    policy_name = f"Policy_{req.schema_name}_{req.table_name}"
    validate_identifier(policy_name, "Policy name")
    policy_q = quote_identifier(policy_name)

    all_cols = {c.lower(): c for c in existing_columns}
    allowed_cols = [all_cols[c.lower()] for c in req.allowed_columns if c.lower() in all_cols]
    if req.allowed_columns and len(allowed_cols) != len(req.allowed_columns):
        raise ValueError("One or more allowedColumns are not present on target table.")

    restricted_cols = [c for c in existing_columns if c not in allowed_cols] if allowed_cols else []

    statements: list[tuple[str, list[Any]]] = []
    statements.extend(bootstrap_rls_sql())

    statements.append(
        (
            f"""
            IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = '{role_name}' AND type = 'R')
                EXEC('CREATE ROLE {role_q};');
            """,
            [],
        )
    )

    if req.table_access:
        statements.append((f"GRANT SELECT ON {schema_q}.{table_q} TO {role_q};", []))
    else:
        statements.append((f"REVOKE SELECT ON {schema_q}.{table_q} FROM {role_q};", []))

    for col in restricted_cols:
        col_q = quote_identifier(col)
        statements.append(
            (
                f"DENY SELECT ON OBJECT::{schema_q}.{table_q} ({col_q}) TO {role_q};",
                [],
            )
        )

    statements.append(
        (
            f"""
            IF NOT EXISTS (SELECT 1 FROM sys.security_policies WHERE name = '{policy_name}')
                EXEC('CREATE SECURITY POLICY security.{policy_q}
                      ADD FILTER PREDICATE security.fn_rls_customer_filter(CustomerID)
                      ON {schema_q}.{table_q}
                      WITH (STATE = ON);');
            """,
            [],
        )
    )

    filter_column = req.row_filter.column if req.row_filter else "CustomerID"
    filter_operator = req.row_filter.operator if req.row_filter else "="
    filter_value = req.row_filter.value if req.row_filter else req.customer_id

    statements.append(
        (
            """
            MERGE security.CustomerPolicies AS target
            USING (
                SELECT
                    ? AS CustomerID,
                    ? AS SchemaName,
                    ? AS TableName
            ) AS src
            ON target.CustomerID = src.CustomerID
               AND target.SchemaName = src.SchemaName
               AND target.TableName = src.TableName
            WHEN MATCHED THEN
                UPDATE SET
                    AllowedColumns = ?,
                    FilterColumn = ?,
                    FilterOperator = ?,
                    FilterValue = ?,
                    TableAccess = ?,
                    UpdatedAt = SYSUTCDATETIME()
            WHEN NOT MATCHED THEN
                INSERT (CustomerID, SchemaName, TableName, AllowedColumns, FilterColumn, FilterOperator, FilterValue, TableAccess, UpdatedAt)
                VALUES (src.CustomerID, src.SchemaName, src.TableName, ?, ?, ?, ?, ?, SYSUTCDATETIME());
            """,
            [
                req.customer_id,
                req.schema_name,
                req.table_name,
                ",".join(allowed_cols),
                filter_column,
                filter_operator,
                filter_value,
                1 if req.table_access else 0,
                ",".join(allowed_cols),
                filter_column,
                filter_operator,
                filter_value,
                1 if req.table_access else 0,
            ],
        )
    )

    statements.append(
        ("DELETE FROM security.CustomerIdentityMap WHERE CustomerID = ?;", [req.customer_id])
    )
    for identity in req.identities:
        statements.append(
            (
                """
                INSERT INTO security.CustomerIdentityMap (IdentityOid, IdentityUpn, CustomerID, IsActive, CreatedAt, UpdatedAt)
                VALUES (?, ?, ?, 1, SYSUTCDATETIME(), SYSUTCDATETIME());
                """,
                [identity.oid, identity.upn, req.customer_id],
            )
        )

    return statements


def build_delete_policy_sql(customer_id: str) -> list[tuple[str, list[Any]]]:
    role_name = f"cust_{customer_id}".replace("-", "_")
    validate_identifier(role_name, "Derived role name")
    role_q = quote_identifier(role_name)
    return [
        ("DELETE FROM security.CustomerPolicies WHERE CustomerID = ?;", [customer_id]),
        ("DELETE FROM security.CustomerIdentityMap WHERE CustomerID = ?;", [customer_id]),
        (
            f"""
            IF EXISTS (SELECT 1 FROM sys.database_principals WHERE name = '{role_name}' AND type = 'R')
                EXEC('DROP ROLE {role_q}');
            """,
            [],
        ),
    ]


def build_get_policy_sql(customer_id: str) -> list[tuple[str, list[Any]]]:
    return [
        (
            """
            SELECT CustomerID, SchemaName, TableName, AllowedColumns, FilterColumn, FilterOperator, FilterValue, TableAccess, UpdatedAt
            FROM security.CustomerPolicies
            WHERE CustomerID = ?;
            """,
            [customer_id],
        ),
        (
            """
            SELECT IdentityOid, IdentityUpn, CustomerID, IsActive, UpdatedAt
            FROM security.CustomerIdentityMap
            WHERE CustomerID = ?;
            """,
            [customer_id],
        ),
    ]


def to_debug_sql(statements: list[tuple[str, list[Any]]]) -> list[dict[str, Any]]:
    return [{"sql": sql.strip(), "params": params} for sql, params in statements]


def build_list_metadata_sql() -> tuple[str, list[Any]]:
    return (
        """
        SELECT
            c.TABLE_SCHEMA,
            c.TABLE_NAME,
            c.COLUMN_NAME,
            c.DATA_TYPE,
            c.ORDINAL_POSITION
        FROM INFORMATION_SCHEMA.COLUMNS c
        INNER JOIN INFORMATION_SCHEMA.TABLES t
            ON c.TABLE_SCHEMA = t.TABLE_SCHEMA
           AND c.TABLE_NAME = t.TABLE_NAME
        WHERE t.TABLE_TYPE = 'BASE TABLE'
        ORDER BY c.TABLE_SCHEMA, c.TABLE_NAME, c.ORDINAL_POSITION;
        """,
        [],
    )


def build_sample_rows_sql(schema_name: str, table_name: str, top_n: int) -> tuple[str, list[Any]]:
    validate_identifier(schema_name, "schemaName")
    validate_identifier(table_name, "tableName")
    if top_n <= 0:
        raise ValueError("top must be greater than 0.")
    if top_n > 100:
        raise ValueError("top cannot be greater than 100.")

    schema_q = quote_identifier(schema_name)
    table_q = quote_identifier(table_name)
    sql = f"SELECT TOP ({top_n}) * FROM {schema_q}.{table_q};"
    return sql, []


def build_policy_overlay_sql(customer_id: str) -> list[tuple[str, list[Any]]]:
    return [
        (
            """
            SELECT
                CustomerID,
                SchemaName,
                TableName,
                AllowedColumns,
                FilterColumn,
                FilterOperator,
                FilterValue,
                TableAccess,
                UpdatedAt
            FROM security.CustomerPolicies
            WHERE CustomerID = ?
            ORDER BY SchemaName, TableName;
            """,
            [customer_id],
        ),
        (
            """
            SELECT
                IdentityOid,
                IdentityUpn,
                CustomerID,
                IsActive,
                UpdatedAt
            FROM security.CustomerIdentityMap
            WHERE CustomerID = ?
            ORDER BY IdentityOid;
            """,
            [customer_id],
        ),
    ]


def build_filter_fields_sql(schema_name: str, table_name: str) -> tuple[str, list[Any]]:
    validate_identifier(schema_name, "schemaName")
    validate_identifier(table_name, "tableName")
    return (
        """
        SELECT
            COLUMN_NAME,
            DATA_TYPE,
            ORDINAL_POSITION
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION;
        """,
        [schema_name, table_name],
    )
