class FileProcessingError(Exception):
    """Custom exception for errors related to file processing."""
    def __init__(self, message, errors=None):
        super().__init__(message)
        self.message = message
        self.errors = errors

    def __str__(self):
        return f"{self.message} | Errors: {self.errors}"
