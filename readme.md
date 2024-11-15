# Heart Rate Variability (HRV) Data Processing and Analysis

This project involves processing and analyzing heart rate data to calculate Heart Rate Variability (HRV). The data is loaded from Google Cloud Storage (GCS), processed, and the results are stored in Google BigQuery for further analysis. The primary objective is to preprocess raw heart rate data, interpolate missing values, and calculate the HRV using the RMSSD (Root Mean Square of Successive Differences) method.

## Table of Contents
- [Overview](#overview)
- [Technologies Used](#technologies-used)
- [Setup](#setup)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
- [Usage](#usage)
  - [Running the Project](#running-the-project)
- [Modules](#modules)
  - [DataPreprocessor](#datapreprocessor)
  - [HRVCalculator](#hrvcalculator)
  - [DataLoader](#dataloader)
  - [Exception Handling](#exception-handling)
- [Contributing](#contributing)
- [License](#license)

## Overview

This project provides an end-to-end solution for heart rate variability analysis. The solution includes:

Data Preprocessing: It filters out invalid data (e.g., zero heartbeat values), interpolates missing data, and computes the time differences between consecutive heartbeat intervals.
HRV Calculation: The RMSSD method is used to calculate HRV values from the heart rate data, which are subsequently stored in BigQuery.
Data Loading: The processed and HRV-calculated data is loaded into Google BigQuery for storage and further analysis.

## Technologies Used

- Python 3.7+
- Google Cloud SDK (for interacting with GCS and BigQuery)
- pandas (for data manipulation)
- numpy (for numerical operations)
- google-cloud-bigquery (for interacting with BigQuery)
- google-cloud-storage (for interacting with Google Cloud Storage)
- logging (for logging)
- functions-framework (for cloud event-driven execution)

## Setup

### Prerequisites
1. Python 3.7 or higher
2. Google Cloud Project with access to BigQuery and Google Cloud Storage
3. Google Cloud SDK installed and configured

### Installation

1. Clone the repository:
    ``` bash
    git clone https://github.com/DataArize/prj02-dog-hrv-analyzer.git
    cd prj02-dog-hrv-analyzer
    ```
2. Create a virtual environment:
    ``` bash
    python3 -m venv venv
    source venv/bin/activate
    ```
3. Install dependencies:
    ``` bash
    pip install -r requirements.txt
    ```
4. Set up your Google Cloud credentials: Make sure to authenticate with Google Cloud using a service account with the required permissions:
    ``` bash
    gcloud auth activate-service-account --key-file=path/to/your-service-account-key.json
    ```

## Usage

### Running the Project

1. To run the code locally or within the cloud environment, use the main entry point in `process_sensor_data.py`:
    ``` bash
    python process_sensor_data.py
    ```
2. Alternatively, if you're deploying the project to Google Cloud Functions, ensure the function is triggered correctly by events (e.g., when new data is uploaded to GCS).

## Modules
### DataPreprocessor

The `DataPreprocessor` class handles the filtering, time gap calculations, and interpolation of missing heart rate data.

**Key Methods:**

- `preprocess_data:` Filters and preprocesses raw data for heart rate intervals.
- `filter_rows_with_zero_heartbeat:` Removes rows where heartbeat is zero.
- `filter_rows_with_time_diff_greater:` Filters rows where the time gap between two heartbeats exceeds a specified threshold.
- `interpolate_data:` Interpolates missing data (method is defined but not yet implemented).

### HRVCalculator
The HRVCalculator class calculates Heart Rate Variability (HRV) based on the Root Mean Square of Successive Differences (RMSSD).

**Key Methods:**
- `calculate_rmssd:` Computes RMSSD for a given series of heart rate intervals.
- `calculate_hrv:` Calculates HRV for a DataFrame by applying the RMSSD calculation over time intervals.

### DataLoader
The DataLoader class is responsible for loading the processed data into Google BigQuery.

**Key Methods:**

- `load_data:` Loads the processed data from a Pandas DataFrame into a specified BigQuery table.
- `Handles BadRequest, Forbidden,` and other Google Cloud errors by logging them and raising custom exceptions.

### Exception Handling

The project utilizes custom exceptions for error handling:

- DataLoadError: Raised during any failure in loading data to BigQuery.
- FetchMaxTimestampError: Raised if there is an error in fetching the max timestamp from BigQuery.

## Contributing
We welcome contributions! Please fork the repository and submit a pull request with the changes. Make sure to follow the existing coding standards, and ensure that the code is well-tested.

## License

This project is licensed under the MIT License - see the LICENSE file for details.