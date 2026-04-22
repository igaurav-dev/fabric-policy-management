from dataclasses import dataclass, field
from typing import Any


@dataclass
class RowFilter:
    column: str
    operator: str
    value: str


@dataclass
class IdentityMapping:
    oid: str
    upn: str | None = None


@dataclass
class PolicyRequest:
    customer_id: str
    schema_name: str
    table_name: str
    allowed_columns: list[str] = field(default_factory=list)
    row_filter: RowFilter | None = None
    table_access: bool = True
    identities: list[IdentityMapping] = field(default_factory=list)

    @classmethod
    def from_dict(
        cls,
        data: dict[str, Any],
        route_customer_id: str | None = None,
    ) -> "PolicyRequest":
        customer_id = route_customer_id or data.get("customerId")
        if not customer_id:
            raise ValueError("customerId is required.")

        schema_name = data.get("schemaName")
        table_name = data.get("tableName")
        if not schema_name or not table_name:
            raise ValueError("schemaName and tableName are required.")

        raw_allowed_columns = data.get("allowedColumns", [])
        if not isinstance(raw_allowed_columns, list):
            raise ValueError("allowedColumns must be an array.")

        raw_table_access = data.get("tableAccess", True)
        if not isinstance(raw_table_access, bool):
            raise ValueError("tableAccess must be boolean.")

        raw_filter = data.get("rowFilter")
        row_filter = None
        if raw_filter is not None:
            if not isinstance(raw_filter, dict):
                raise ValueError("rowFilter must be an object.")
            for key in ("column", "operator", "value"):
                if key not in raw_filter or raw_filter[key] in (None, ""):
                    raise ValueError(f"rowFilter.{key} is required.")
            row_filter = RowFilter(
                column=str(raw_filter["column"]),
                operator=str(raw_filter["operator"]),
                value=str(raw_filter["value"]),
            )

        identities = []
        raw_identities = data.get("identities", [])
        if not isinstance(raw_identities, list):
            raise ValueError("identities must be an array.")
        for item in raw_identities:
            if not isinstance(item, dict):
                raise ValueError("Each identity must be an object.")
            oid = str(item.get("oid", "")).strip()
            if not oid:
                raise ValueError("identity.oid is required.")
            upn = item.get("upn")
            identities.append(IdentityMapping(oid=oid, upn=str(upn) if upn else None))

        return cls(
            customer_id=str(customer_id),
            schema_name=str(schema_name),
            table_name=str(table_name),
            allowed_columns=[str(col) for col in raw_allowed_columns],
            row_filter=row_filter,
            table_access=raw_table_access,
            identities=identities,
        )
