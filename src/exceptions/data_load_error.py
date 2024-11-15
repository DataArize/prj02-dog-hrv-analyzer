from typing import Optional


class DataLoadError(Exception):
    def __init__(self, message: str, errors: Optional[Exception] = None):
        super().__init__(message)
        self.errors = errors
        self.message = message
        if errors:
            self.message += f" | Error Details: {errors}"

    def __str__(self):
        return self.message