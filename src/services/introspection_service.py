from __future__ import annotations

from typing import Any

from src.db import open_connection, query_rows
from src.sql_builders import (
    build_filter_fields_sql,
    build_list_metadata_sql,
    build_policy_overlay_sql,
    build_sample_rows_sql,
)


class IntrospectionService:
    def list_metadata(self) -> dict[str, Any]:
        with open_connection() as conn:
            sql, params = build_list_metadata_sql()
            rows = query_rows(conn, sql, params)

            schemas: dict[str, dict[str, Any]] = {}
            for row in rows:
                schema_name = str(row["TABLE_SCHEMA"])
                table_name = str(row["TABLE_NAME"])
                column = {
                    "columnName": str(row["COLUMN_NAME"]),
                    "dataType": str(row["DATA_TYPE"]),
                    "ordinalPosition": int(row["ORDINAL_POSITION"]),
                }

                if schema_name not in schemas:
                    schemas[schema_name] = {"schemaName": schema_name, "tables": {}}
                tables = schemas[schema_name]["tables"]
                if table_name not in tables:
                    tables[table_name] = {"tableName": table_name, "columns": []}
                tables[table_name]["columns"].append(column)

            result = []
            for schema in schemas.values():
                result.append(
                    {
                        "schemaName": schema["schemaName"],
                        "tables": list(schema["tables"].values()),
                    }
                )
            result.sort(key=lambda x: x["schemaName"])
            return {"schemas": result}

    def sample_rows(self, schema_name: str, table_name: str, top_n: int) -> dict[str, Any]:
        with open_connection() as conn:
            verify_sql = """
                SELECT 1
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?;
            """
            exists = query_rows(conn, verify_sql, [schema_name, table_name])
            if not exists:
                raise ValueError(f"Table {schema_name}.{table_name} does not exist.")

            sql, params = build_sample_rows_sql(schema_name, table_name, top_n)
            rows = query_rows(conn, sql, params)
            return {
                "schemaName": schema_name,
                "tableName": table_name,
                "top": top_n,
                "rows": rows,
            }

    def policy_overlay(self, customer_id: str) -> dict[str, Any]:
        with open_connection() as conn:
            policy_sql, identity_sql = build_policy_overlay_sql(customer_id)
            policies = query_rows(conn, policy_sql[0], policy_sql[1])
            identities = query_rows(conn, identity_sql[0], identity_sql[1])
            return {
                "customerId": customer_id,
                "policies": policies,
                "identities": identities,
            }

    def filter_fields(self, schema_name: str, table_name: str) -> dict[str, Any]:
        with open_connection() as conn:
            sql, params = build_filter_fields_sql(schema_name, table_name)
            rows = query_rows(conn, sql, params)
            if not rows:
                raise ValueError(f"No columns found for {schema_name}.{table_name}.")
            return {
                "schemaName": schema_name,
                "tableName": table_name,
                "fields": [
                    {
                        "columnName": str(r["COLUMN_NAME"]),
                        "dataType": str(r["DATA_TYPE"]),
                        "ordinalPosition": int(r["ORDINAL_POSITION"]),
                    }
                    for r in rows
                ],
            }
