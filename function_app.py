import azure.functions as func

from src.http_handlers import (
    delete_policy_handler,
    dry_run_policy_handler,
    get_policy_handler,
    upsert_policy_handler,
)


app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)


@app.route(route="policies", methods=["POST"])
def create_policy(req: func.HttpRequest) -> func.HttpResponse:
    return upsert_policy_handler(req, route_customer_id=None, is_update=False)


@app.route(route="policies/{customerId}", methods=["PUT"])
def update_policy(req: func.HttpRequest) -> func.HttpResponse:
    return upsert_policy_handler(
        req,
        route_customer_id=req.route_params.get("customerId"),
        is_update=True,
    )


@app.route(route="policies/{customerId}", methods=["DELETE"])
def delete_policy(req: func.HttpRequest) -> func.HttpResponse:
    return delete_policy_handler(req, req.route_params.get("customerId"))


@app.route(route="policies/{customerId}", methods=["GET"])
def get_policy(req: func.HttpRequest) -> func.HttpResponse:
    return get_policy_handler(req, req.route_params.get("customerId"))


@app.route(route="policies/{customerId}/dry-run", methods=["POST"])
def dry_run_policy(req: func.HttpRequest) -> func.HttpResponse:
    return dry_run_policy_handler(req, req.route_params.get("customerId"))
