import functions_framework
import pandas as pd

from constants.config_contants import EVENT_DATA_FILE_PLACEHOLDER, EVENT_DATA_BUCKET_PLACEHOLDER, START_TIMESTAMP, \
    END_TIMESTAMP, TIMESTAMP_STR_KEY, NEXT_TIMESTAMP_STR_KEY, TIMESTAMP_FORMAT, DEVICE
from bigquery.bigquery_client import BigQueryClient
from config.settings import Settings
from exceptions.empty_data_error import EmptyDataError
from hrv.hrv_calculation import HRVCalculator
from preprocessor.data_preprocessor import DataPreprocessor
from storage.storage_client import StorageClient
from utils.logger import create_logger

logger = create_logger(__name__)
storage_client = StorageClient()
bigquery_client = BigQueryClient()
data_preprocessor = DataPreprocessor()
hrv_calculator = HRVCalculator()

@functions_framework.cloud_event
def process_sensor_data(cloud_event):
    logger.info(f"Cloud event received, event: ${cloud_event}")
    file_name = cloud_event[EVENT_DATA_FILE_PLACEHOLDER]
    bucket_name = cloud_event[EVENT_DATA_BUCKET_PLACEHOLDER]
    logger.info(f"File name: ${file_name}, Bucket name: {bucket_name}")
    data_frame = storage_client.fetch_and_validate_csv(bucket_name, file_name)
    data_frame[START_TIMESTAMP] = pd.to_datetime(data_frame[TIMESTAMP_STR_KEY], format=TIMESTAMP_FORMAT, utc=True,
                                         errors='coerce')
    data_frame[END_TIMESTAMP] = pd.to_datetime(data_frame[NEXT_TIMESTAMP_STR_KEY], format=TIMESTAMP_FORMAT, utc=True,
                                       errors='coerce')
    for device_id in data_frame[DEVICE].unique():
        device_df = data_frame[data_frame[DEVICE] == device_id]
        max_timestamp = bigquery_client.fetch_max_timestamp_for_sensor(
            sensor_id=device_id,
            dataset_id=Settings.DATASET_ID,
            table_id=Settings.TABLE_ID
        )
        max_timestamp = pd.to_datetime(max_timestamp, utc=True, errors='coerce')
        try:
            delta_df = data_preprocessor.preprocess_data(
                device_df=device_df,
                max_timestamp=max_timestamp,
                device_id=device_id
            )
        except EmptyDataError as ex:
            return
        interpolated_df = data_preprocessor.interpolate_data(
            device_id=device_id,
            delta_df=delta_df
        )
        interpolated_df = hrv_calculator.calculate_hrv(interpolated_df=interpolated_df)
        bigquery_client.load_data(
            interpolated_df=interpolated_df,
            dataset_id=Settings.DATASET_ID,
            table_id=Settings.TABLE_ID,
            write_disposition='WRITE_APPEND'
        )