from io import StringIO

import pandas as pd
from google.cloud import storage
from constants.config_contants import FILE_NAME_PREFIX, FILE_EXTENSIONS, COLUMNS
from google.cloud.exceptions import NotFound, Forbidden, GoogleCloudError
from exceptions.file_processing_error import FileProcessingError
from utils.logger import create_logger


class StorageClient:
    def __init__(self):
        self.client = storage.Client()
        self.logger = create_logger(__name__)

    def fetch_and_validate_csv(self, bucket_name, file_name):
        if not (file_name.startswith(FILE_NAME_PREFIX) and file_name.endswith(FILE_EXTENSIONS)):
            self.logger.info(f"Skipping file {file_name} as it does not match the required pattern.")
            return
        bucket = self.client.bucket(bucket_name)
        source_blob = bucket.blob(file_name)
        try:
            csv_data = source_blob.download_as_text()
            df = pd.read_csv(StringIO(csv_data), names=COLUMNS)
            df.dropna(inplace=True)
            return df
        except NotFound as e:
            raise FileProcessingError(f"File {file_name} not found in bucket {bucket_name}", e)
        except Forbidden as e:
            raise FileProcessingError(f"Access denied to file {file_name} in bucket {bucket_name}", e)
        except GoogleCloudError as e:
            raise FileProcessingError(f"Google Cloud error while accessing {file_name}", e)
        except pd.errors.ParserError as e:
            raise FileProcessingError(f"Error parsing CSV file {file_name}", e)
        except ValueError as e:
            raise FileProcessingError(f"Value error while processing CSV {file_name}", e)
        except Exception as e:
            raise FileProcessingError(f"Unexpected error while processing file {file_name}", e)
