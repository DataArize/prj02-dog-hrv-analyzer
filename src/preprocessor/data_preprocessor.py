from constants.config_contants import START_TIMESTAMP, END_TIMESTAMP
from utils.logger import create_logger
import pandas as pd
from typing import Optional
import logging
from src.exceptions.empty_data_error import EmptyDataError


class DataPreprocessor:

    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or create_logger(__name__)

    def preprocess_data(self, device_df: pd.DataFrame,
                        max_timestamp: pd.Timestamp,
                        device_id: str) -> pd.DataFrame:

        if not isinstance(device_df, pd.DataFrame):
            self.logger.error("Input device_df is not a valid pandas DataFrame.")
            raise ValueError("Input device_df should be a pandas DataFrame")
        self.logger.info(f"Started preprocessing data for device {device_id}.")
        delta_df = device_df[device_df[START_TIMESTAMP] > max_timestamp]
        if delta_df.empty:
            self.logger.info(f"No new data for device {device_id}.")
            raise EmptyDataError(f"No new data for device {device_id}")

        self.logger.info(f"DataFrame size before filtering: {delta_df.shape[0]} rows")
        delta_df = delta_df.sort_values(by=START_TIMESTAMP)
        delta_df['time_diff'] = (delta_df[END_TIMESTAMP] - delta_df[START_TIMESTAMP]).dt.total_seconds()
        delta_df = self.filter_rows_with_zero_heartbeat(delta_df)
        delta_df = self.filter_rows_with_time_diff_greater(delta_df)
        delta_df['new_time_diff'] = (delta_df[END_TIMESTAMP] - delta_df[START_TIMESTAMP]).dt.total_seconds()
        self.logger.info(f"DataFrame size after filtering: {delta_df.shape[0]} rows")
        return delta_df

    def filter_rows_with_zero_heartbeat(self, delta_df: pd.DataFrame) -> pd.DataFrame:

        if 'heartbeat' not in delta_df.columns:
            self.logger.error("Column 'heartbeat' not found in the DataFrame.")
            raise KeyError("Column 'heartbeat' not found in the DataFrame.")
        return delta_df[delta_df['heartbeat'] != 0]

    def filter_rows_with_time_diff_greater(self, delta_df: pd.DataFrame) -> pd.DataFrame:

        return delta_df[delta_df['time_diff'] < 1.5]

    def interpolate_data(self, delta_df: pd.DataFrame, device_id: str) -> pd.DataFrame:

        self.logger.info("Interpolating missing data.")
        interpolated_rows = []
        for i, row in delta_df.iterrows():
            if row['new_time_diff'] > 1:
                missing_rows = int(row['new_time_diff']) - 1
                for j in range(missing_rows):
                    interpolated_row = row.copy()
                    interpolated_row[START_TIMESTAMP] = row[START_TIMESTAMP] + pd.Timedelta(seconds=j + 1)
                    interpolated_rows.append(interpolated_row)
            interpolated_rows.append(row)

        interpolated_df = pd.DataFrame(interpolated_rows)
        self.logger.info(f"Interpolated missing data for device {device_id}. Total rows: {len(interpolated_df)}")
        return interpolated_df
