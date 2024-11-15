class FetchMaxTimestampError(Exception):
    """Custom exception for errors when fetching max timestamp for sensor."""

    def __init__(self, message, sensor_id=None, errors=None):
        super().__init__(message)
        self.message = message
        self.sensor_id = sensor_id
        self.errors = errors

    def __str__(self):
        sensor_info = f" for sensor {self.sensor_id}" if self.sensor_id else ""
        return f"{self.message}{sensor_info} | Errors: {self.errors}"
