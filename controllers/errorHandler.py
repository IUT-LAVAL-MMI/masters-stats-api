import traceback
from typing import TypedDict, Optional
import werkzeug
from flask import Blueprint
from werkzeug.exceptions import InternalServerError, HTTPException, NotFound
from flask import current_app

__all__ = ['error_handler']

error_handler = Blueprint('error', __name__)


class ErrorMessage(TypedDict):
    error: str
    details: Optional[str]
    code: int
    type: Optional[str]


@error_handler.app_errorhandler(ValueError)
@error_handler.app_errorhandler(werkzeug.exceptions.BadRequest)
def handle_bad_request(e):
    print(traceback.format_exc())
    return ErrorMessage(error='bad request', details=str(e), code=400), 400


@error_handler.app_errorhandler(NotFound)
def handle_not_found(e):
    return ErrorMessage(error='resource not found', details=str(e), code=404), 404


@error_handler.app_errorhandler(HTTPException)
def handle_other_http_exception(e: HTTPException):
    return ErrorMessage(error=e.description, details=str(e), code=e.code), e.code


@error_handler.app_errorhandler(Exception)
def handle_other_exception(e: Exception):
    print(traceback.format_exc())
    current_app.logger.warning("Unmanaged error: %s.", str(e))
    return ErrorMessage(error="Unmanaged error", details=str(e), type=type(e).__name__, code=500), 500
