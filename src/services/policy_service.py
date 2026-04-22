from __future__ import annotations

from typing import Any

from src.db import execute_batch, open_connection, query_rows
from src.models import PolicyRequest
from src.sql_builders import (
    build_delete_policy_sql,
    build_get_policy_sql,
    build_upsert_policy_sql,
    ensure_table_and_columns_sql,
    to_debug_sql,
)


class PolicyService:
    def upsert_policy(self, req: PolicyRequest, dry_run: bool = False) -> dict[str, Any]:
        with open_connection() as conn:
            table_sql, columns_sql = ensure_table_and_columns_sql(req)
            table_rows = query_rows(conn, table_sql[0], table_sql[1])
            if not table_rows:
                raise ValueError(f"Table {req.schema_name}.{req.table_name} does not exist.")

            col_rows = query_rows(conn, columns_sql[0], columns_sql[1])
            existing_columns = [str(row["COLUMN_NAME"]) for row in col_rows]
            if "CustomerID".lower() not in {c.lower() for c in existing_columns}:
                raise ValueError(
                    f"Table {req.schema_name}.{req.table_name} must contain CustomerID column for RLS predicate."
                )

            statements = build_upsert_policy_sql(req, existing_columns)
            if dry_run:
                return {"executed": False, "statements": to_debug_sql(statements)}

            execute_batch(conn, statements)
            return {"executed": True, "statementCount": len(statements)}

    def delete_policy(self, customer_id: str) -> dict[str, Any]:
        with open_connection() as conn:
            statements = build_delete_policy_sql(customer_id)
            execute_batch(conn, statements)
            return {"executed": True, "statementCount": len(statements)}

    def get_policy(self, customer_id: str) -> dict[str, Any]:
        with open_connection() as conn:
            policy_sql, identity_sql = build_get_policy_sql(customer_id)
            policy_rows = query_rows(conn, policy_sql[0], policy_sql[1])
            identity_rows = query_rows(conn, identity_sql[0], identity_sql[1])
            return {
                "customerId": customer_id,
                "policies": policy_rows,
                "identities": identity_rows,
            }
