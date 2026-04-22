from __future__ import annotations

import json
import logging
import uuid
from typing import Any

import azure.functions as func

from src.models import PolicyRequest
from src.services.introspection_service import IntrospectionService
from src.services.policy_service import PolicyService


logger = logging.getLogger("fabric-policy-api")
service = PolicyService()
introspection_service = IntrospectionService()


def _json_response(body: dict[str, Any], status: int = 200) -> func.HttpResponse:
    return func.HttpResponse(
        json.dumps(body, default=str),
        status_code=status,
        mimetype="application/json",
    )


def _parse_json(req: func.HttpRequest) -> dict[str, Any]:
    try:
        payload = req.get_json()
        if not isinstance(payload, dict):
            raise ValueError("JSON payload must be an object.")
        return payload
    except ValueError as exc:
        raise ValueError(f"Invalid JSON body: {exc}") from exc


def upsert_policy_handler(
    req: func.HttpRequest,
    route_customer_id: str | None,
    is_update: bool,
) -> func.HttpResponse:
    correlation_id = str(uuid.uuid4())
    try:
        payload = _parse_json(req)
        model = PolicyRequest.from_dict(payload, route_customer_id=route_customer_id)
        result = service.upsert_policy(model, dry_run=False)
        return _json_response(
            {
                "success": True,
                "message": "Policy updated." if is_update else "Policy created.",
                "details": result,
                "correlationId": correlation_id,
            },
            status=200 if is_update else 201,
        )
    except ValueError as exc:
        return _json_response(
            {
                "success": False,
                "message": "Validation failed.",
                "details": str(exc),
                "correlationId": correlation_id,
            },
            status=400,
        )
    except Exception as exc:
        logger.exception("Failed to upsert policy. correlationId=%s", correlation_id)
        return _json_response(
            {
                "success": False,
                "message": "Internal server error.",
                "details": str(exc),
                "correlationId": correlation_id,
            },
            status=500,
        )


def delete_policy_handler(req: func.HttpRequest, customer_id: str | None) -> func.HttpResponse:
    correlation_id = str(uuid.uuid4())
    if not customer_id:
        return _json_response(
            {
                "success": False,
                "message": "customerId route parameter is required.",
                "details": None,
                "correlationId": correlation_id,
            },
            status=400,
        )

    try:
        result = service.delete_policy(customer_id)
        return _json_response(
            {
                "success": True,
                "message": "Policy deleted.",
                "details": result,
                "correlationId": correlation_id,
            },
            status=200,
        )
    except Exception as exc:
        logger.exception("Failed to delete policy. correlationId=%s", correlation_id)
        return _json_response(
            {
                "success": False,
                "message": "Internal server error.",
                "details": str(exc),
                "correlationId": correlation_id,
            },
            status=500,
        )


def get_policy_handler(req: func.HttpRequest, customer_id: str | None) -> func.HttpResponse:
    correlation_id = str(uuid.uuid4())
    if not customer_id:
        return _json_response(
            {
                "success": False,
                "message": "customerId route parameter is required.",
                "details": None,
                "correlationId": correlation_id,
            },
            status=400,
        )

    try:
        result = service.get_policy(customer_id)
        return _json_response(
            {
                "success": True,
                "message": "Policy fetched.",
                "details": result,
                "correlationId": correlation_id,
            },
            status=200,
        )
    except Exception as exc:
        logger.exception("Failed to get policy. correlationId=%s", correlation_id)
        return _json_response(
            {
                "success": False,
                "message": "Internal server error.",
                "details": str(exc),
                "correlationId": correlation_id,
            },
            status=500,
        )


def dry_run_policy_handler(req: func.HttpRequest, customer_id: str | None) -> func.HttpResponse:
    correlation_id = str(uuid.uuid4())
    try:
        payload = _parse_json(req)
        model = PolicyRequest.from_dict(payload, route_customer_id=customer_id)
        result = service.upsert_policy(model, dry_run=True)
        return _json_response(
            {
                "success": True,
                "message": "Dry-run generated SQL successfully.",
                "details": result,
                "correlationId": correlation_id,
            },
            status=200,
        )
    except ValueError as exc:
        return _json_response(
            {
                "success": False,
                "message": "Validation failed.",
                "details": str(exc),
                "correlationId": correlation_id,
            },
            status=400,
        )
    except Exception as exc:
        logger.exception("Failed to dry-run policy. correlationId=%s", correlation_id)
        return _json_response(
            {
                "success": False,
                "message": "Internal server error.",
                "details": str(exc),
                "correlationId": correlation_id,
            },
            status=500,
        )


def introspect_metadata_handler(req: func.HttpRequest) -> func.HttpResponse:
    correlation_id = str(uuid.uuid4())
    try:
        result = introspection_service.list_metadata()
        return _json_response(
            {
                "success": True,
                "message": "Metadata fetched.",
                "details": result,
                "correlationId": correlation_id,
            },
            status=200,
        )
    except Exception as exc:
        logger.exception("Failed to introspect metadata. correlationId=%s", correlation_id)
        return _json_response(
            {
                "success": False,
                "message": "Internal server error.",
                "details": str(exc),
                "correlationId": correlation_id,
            },
            status=500,
        )


def introspect_sample_handler(req: func.HttpRequest) -> func.HttpResponse:
    correlation_id = str(uuid.uuid4())
    schema_name = (req.params.get("schemaName") or "").strip()
    table_name = (req.params.get("tableName") or "").strip()
    top_raw = (req.params.get("top") or "20").strip()

    if not schema_name or not table_name:
        return _json_response(
            {
                "success": False,
                "message": "schemaName and tableName query params are required.",
                "details": None,
                "correlationId": correlation_id,
            },
            status=400,
        )
    try:
        top_n = int(top_raw)
    except ValueError:
        return _json_response(
            {
                "success": False,
                "message": "top must be a valid integer.",
                "details": None,
                "correlationId": correlation_id,
            },
            status=400,
        )

    try:
        result = introspection_service.sample_rows(schema_name, table_name, top_n)
        return _json_response(
            {
                "success": True,
                "message": "Sample rows fetched.",
                "details": result,
                "correlationId": correlation_id,
            },
            status=200,
        )
    except ValueError as exc:
        return _json_response(
            {
                "success": False,
                "message": "Validation failed.",
                "details": str(exc),
                "correlationId": correlation_id,
            },
            status=400,
        )
    except Exception as exc:
        logger.exception("Failed to introspect sample rows. correlationId=%s", correlation_id)
        return _json_response(
            {
                "success": False,
                "message": "Internal server error.",
                "details": str(exc),
                "correlationId": correlation_id,
            },
            status=500,
        )


def policy_overlay_handler(req: func.HttpRequest, customer_id: str | None) -> func.HttpResponse:
    correlation_id = str(uuid.uuid4())
    if not customer_id:
        return _json_response(
            {
                "success": False,
                "message": "customerId route parameter is required.",
                "details": None,
                "correlationId": correlation_id,
            },
            status=400,
        )
    try:
        result = introspection_service.policy_overlay(customer_id)
        return _json_response(
            {
                "success": True,
                "message": "Policy overlay fetched.",
                "details": result,
                "correlationId": correlation_id,
            },
            status=200,
        )
    except Exception as exc:
        logger.exception("Failed to fetch policy overlay. correlationId=%s", correlation_id)
        return _json_response(
            {
                "success": False,
                "message": "Internal server error.",
                "details": str(exc),
                "correlationId": correlation_id,
            },
            status=500,
        )


def filter_fields_handler(req: func.HttpRequest) -> func.HttpResponse:
    correlation_id = str(uuid.uuid4())
    schema_name = (req.params.get("schemaName") or "").strip()
    table_name = (req.params.get("tableName") or "").strip()

    if not schema_name or not table_name:
        return _json_response(
            {
                "success": False,
                "message": "schemaName and tableName query params are required.",
                "details": None,
                "correlationId": correlation_id,
            },
            status=400,
        )
    try:
        result = introspection_service.filter_fields(schema_name, table_name)
        return _json_response(
            {
                "success": True,
                "message": "Filter fields fetched.",
                "details": result,
                "correlationId": correlation_id,
            },
            status=200,
        )
    except ValueError as exc:
        return _json_response(
            {
                "success": False,
                "message": "Validation failed.",
                "details": str(exc),
                "correlationId": correlation_id,
            },
            status=400,
        )
    except Exception as exc:
        logger.exception("Failed to fetch filter fields. correlationId=%s", correlation_id)
        return _json_response(
            {
                "success": False,
                "message": "Internal server error.",
                "details": str(exc),
                "correlationId": correlation_id,
            },
            status=500,
        )
