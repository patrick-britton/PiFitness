"""Plain domain exceptions for the library/executioner layer.

These exceptions have no dependency on FastAPI or any web framework.
Routers and entry points translate them into HTTP responses as needed.
"""


class TaskExecutionError(Exception):
    """Raised when a background task execution fails.

    Carries a human-readable detail message. Routers should catch this
    and convert it to an appropriate HTTP response (e.g. HTTPException 500).
    """

    def __init__(self, detail: str):
        self.detail = detail
        super().__init__(detail)