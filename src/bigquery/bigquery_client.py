import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound, Forbidden, BadRequest, GoogleCloudError
from exceptions.fetch_max_timestamp_error import FetchMaxTimestampError
from utils.logger import create_logger
from exceptions.data_load_error import DataLoadError
from constants.config_contants import MAX_TIMESTAMP

from src.constants.config_contants import SENSOR_ID


class BigQueryClient:
    def __init__(self):
        self.client = bigquery.Client()
        self.logger = create_logger(__name__)

    def _build_query(self, sensor_id: str, dataset_id: str, table_id: str) -> str:

        return f"""
            SELECT MAX(timestamp_str) AS max_timestamp
            FROM `{dataset_id}.{table_id}`
            WHERE device = @sensor_id
        """

    def fetch_max_timestamp_for_sensor(self, sensor_id: str, dataset_id: str, table_id: str) -> pd.Timestamp:

        try:
            self.logger.info(
                f"Fetching max timestamp for sensor {sensor_id} from dataset {dataset_id}, table {table_id}.")

            query = self._build_query(sensor_id, dataset_id, table_id)
            job_config = bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter(SENSOR_ID, "STRING", sensor_id)]
            )
            query_job = self.client.query(query, job_config=job_config)
            result = query_job.result().to_dataframe()
            if result.empty or MAX_TIMESTAMP not in result:
                self.logger.warning(f"No valid timestamp found for sensor {sensor_id}. Returning min timestamp.")
                return pd.Timestamp.min

            max_timestamp = result[MAX_TIMESTAMP][0]
            if pd.notnull(max_timestamp):
                self.logger.info(f"Max timestamp for sensor {sensor_id}: {max_timestamp}")
                return max_timestamp
            else:
                self.logger.warning(f"Invalid max timestamp found for sensor {sensor_id}. Returning min timestamp.")
                return pd.Timestamp.min

        except NotFound as e:
            self.logger.error(f"Dataset or table not found for sensor {sensor_id}. Error: {str(e)}")
            raise FetchMaxTimestampError("Dataset or table not found", sensor_id=sensor_id, errors=e)
        except Forbidden as e:
            self.logger.error(f"Access denied for sensor {sensor_id}. Error: {str(e)}")
            raise FetchMaxTimestampError("Access denied to dataset or table", sensor_id=sensor_id, errors=e)
        except BadRequest as e:
            self.logger.error(f"Bad request for sensor {sensor_id}. Error: {str(e)}")
            raise FetchMaxTimestampError("Invalid query or parameters", sensor_id=sensor_id, errors=e)
        except GoogleCloudError as e:
            self.logger.error(f"Google Cloud error occurred for sensor {sensor_id}. Error: {str(e)}")
            raise FetchMaxTimestampError("Google Cloud error occurred", sensor_id=sensor_id, errors=e)
        except KeyError as e:
            self.logger.error(f"Missing expected column in query result for sensor {sensor_id}. Error: {str(e)}")
            raise FetchMaxTimestampError("Missing expected column in query result", sensor_id=sensor_id, errors=e)
        except Exception as e:
            self.logger.error(f"Unexpected error occurred for sensor {sensor_id}. Error: {str(e)}")
            raise FetchMaxTimestampError("An unexpected error occurred", sensor_id=sensor_id, errors=e)

    def load_data(self, interpolated_df, dataset_id: str, table_id: str, write_disposition: str = 'WRITE_APPEND'):

        if interpolated_df.empty:
            self.logger.warning("DataFrame is empty, no data to load.")
            raise DataLoadError("No data to load; DataFrame is empty.")

        if not dataset_id or not table_id:
            self.logger.error("Dataset ID or Table ID is missing.")
            raise DataLoadError("Dataset ID or Table ID is missing.")

        dataset_ref = self.client.get_dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)

        job_config = bigquery.LoadJobConfig(
            write_disposition=getattr(bigquery.WriteDisposition, write_disposition),
        )
        try:
            self.logger.info(f"Starting data load for {len(interpolated_df)} rows into {dataset_id}.{table_id}.")
            job = self.client.load_table_from_dataframe(interpolated_df, table_ref, job_config=job_config)
            self.logger.info(f"Data successfully loaded into {dataset_id}.{table_id}. Job ID: {job.job_id}")

        except BadRequest as e:
            self.logger.error(f"Bad request error while loading data into {dataset_id}.{table_id}: {str(e)}")
            raise DataLoadError(f"Bad request error while loading data into BigQuery", errors=e)

        except Forbidden as e:
            self.logger.error(f"Permission denied while loading data into {dataset_id}.{table_id}: {str(e)}")
            raise DataLoadError(f"Permission denied for {dataset_id}.{table_id}", errors=e)

        except GoogleCloudError as e:
            self.logger.error(f"Google Cloud error occurred during data load: {str(e)}")
            raise DataLoadError(f"Google Cloud error occurred while loading data into BigQuery", errors=e)

        except Exception as e:
            self.logger.error(f"Unexpected error during data load: {str(e)}")
            raise DataLoadError(f"An unexpected error occurred while loading data", errors=e)

