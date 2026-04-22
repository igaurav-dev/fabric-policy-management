from __future__ import annotations

import json
import logging
import uuid
from typing import Any

import azure.functions as func

from src.models import PolicyRequest
from src.services.policy_service import PolicyService


logger = logging.getLogger("fabric-policy-api")
service = PolicyService()


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
