import pandas as pd
import numpy as np
import logging
from typing import Optional
from utils.logger import create_logger
from constants.config_contants import START_TIMESTAMP, END_TIMESTAMP, HRV_RMSSD, RR_INTERVAL_MS, NEXT_TIMESTAMP_STR_KEY, TIMESTAMP_STR_KEY


class HRVCalculator:

    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = create_logger(__name__)

    def calculate_rmssd(self, series: pd.Series) -> float:
        if not isinstance(series, pd.Series):
            self.logger.error("Input series must be a pandas Series.")
            raise ValueError("Input series must be a pandas Series.")

        diffs = series.diff().dropna()
        squared_diffs = diffs ** 2
        rmssd_value = np.sqrt(squared_diffs.mean())
        return rmssd_value

    def calculate_rr_intervals(self, df: pd.DataFrame) -> pd.DataFrame:

        if START_TIMESTAMP not in df or END_TIMESTAMP not in df:
            self.logger.error("DataFrame must contain START_TIMESTAMP and END_TIMESTAMP columns.")
            raise KeyError("DataFrame must contain START_TIMESTAMP and END_TIMESTAMP columns.")

        df[RR_INTERVAL_MS] = (df[END_TIMESTAMP] - df[START_TIMESTAMP]).dt.total_seconds() * 1000
        return df

    def calculate_hrv(self, interpolated_df: pd.DataFrame) -> pd.DataFrame:

        self.logger.info("Started HRV calculation.")
        if not isinstance(interpolated_df, pd.DataFrame):
            self.logger.error("Input interpolated_df is not a valid pandas DataFrame.")
            raise ValueError("Input interpolated_df should be a pandas DataFrame.")
        interpolated_df[START_TIMESTAMP] = pd.to_datetime(interpolated_df[START_TIMESTAMP], errors='coerce')
        interpolated_df[END_TIMESTAMP] = pd.to_datetime(interpolated_df[END_TIMESTAMP], errors='coerce')
        if interpolated_df[START_TIMESTAMP].isnull().any() or interpolated_df[END_TIMESTAMP].isnull().any():
            self.logger.warning("Some timestamps are invalid or missing. These will be handled as NaT (Not a Time).")
        interpolated_df = self.calculate_rr_intervals(interpolated_df)
        self.logger.info("Calculating HRV using RMSSD over a rolling window.")
        interpolated_df[HRV_RMSSD] = (interpolated_df[RR_INTERVAL_MS]
                                      .rolling(window=10).apply(self.calculate_rmssd, raw=False))
        interpolated_df[TIMESTAMP_STR_KEY] = pd.to_datetime(interpolated_df[TIMESTAMP_STR_KEY], errors='coerce')
        interpolated_df[NEXT_TIMESTAMP_STR_KEY] = pd.to_datetime(interpolated_df[NEXT_TIMESTAMP_STR_KEY], errors='coerce')
        self.logger.info("HRV calculation completed.")
        return interpolated_df

