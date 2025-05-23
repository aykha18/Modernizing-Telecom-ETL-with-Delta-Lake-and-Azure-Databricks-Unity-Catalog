import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
from azure.storage.filedatalake import DataLakeServiceClient
import os

# ---- Configuration ----
STORAGE_ACCOUNT_NAME = "YOUR_STORAGE_ACCOUNT_NAME"  # Replace with your actual storage account name
STORAGE_ACCOUNT_KEY = "YOUR_STORAGE_ACCOUNT_KEY"  # Replace with your actual key
FILE_SYSTEM_NAME = "hourlycdr"
FOLDER_PATH = "cdr_data"  # optional subfolder

fake = Faker()

def generate_cdr_data(num_records=10000):
    data = []
    for _ in range(num_records):
        start_time = fake.date_time_this_year()
        duration = np.random.randint(1, 3600)
        end_time = start_time + timedelta(seconds=duration)
        call_type = np.random.choice(['voice', 'SMS', 'data'], p=[0.6, 0.3, 0.1])
        data_usage = np.random.exponential(5) if call_type == 'data' else 0
        data.append({
            'call_id': fake.uuid4(),
            'caller_number': fake.phone_number(),
            'receiver_number': fake.phone_number(),
            'call_start_time': start_time,
            'call_end_time': end_time,
            'call_duration': duration,
            'call_type': call_type,
            'cell_tower_id': fake.bothify(text='TWR-#####'),
            'call_status': np.random.choice(['completed', 'failed'], p=[0.9, 0.1]),
            'data_usage': data_usage,
            'call_date': start_time.date(),
            'call_hour': start_time.hour,
            'weekend_flag': int(start_time.weekday() >= 5)
        })
    return pd.DataFrame(data)

def upload_to_adls(df, file_name):
    try:
        # Authenticate
        service_client = DataLakeServiceClient(
            account_url=f"https://{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net",
            credential=STORAGE_ACCOUNT_KEY
        )
        file_system_client = service_client.get_file_system_client(FILE_SYSTEM_NAME)

        # File path in ADLS
        full_path = f"{FOLDER_PATH}/{file_name}"
        directory_client = file_system_client.get_directory_client(FOLDER_PATH)
        file_client = directory_client.create_file(file_name)

        # Upload CSV content
        csv_content = df.to_csv(index=False)
        file_client.append_data(data=csv_content, offset=0, length=len(csv_content))
        file_client.flush_data(len(csv_content))
        print(f"✅ Uploaded: {file_name}")

    except Exception as e:
        print(f"❌ Upload failed: {e}")

if __name__ == "__main__":
    now = datetime.utcnow()
    timestamp_str = now.strftime("%Y%m%d_%H%M%S")
    file_name = f"cdr_{timestamp_str}.csv"

    df = generate_cdr_data(10000)
    upload_to_adls(df, file_name)
