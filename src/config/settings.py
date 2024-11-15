import os
from src.constants.config_contants import ENV_DATASET_ID, ENV_TABLE_ID

class Settings:
    DATASET_ID = os.getenv(ENV_DATASET_ID)
    TABLE_ID = os.getenv(ENV_TABLE_ID)
