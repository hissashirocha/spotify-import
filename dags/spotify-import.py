from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from google.cloud import storage

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 9),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'spotify_to_gcs',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
)

def get_spotify_data():
    client_credentials_manager = SpotifyClientCredentials(
        client_id='your_client_id',
        client_secret='your_client_secret',
    )
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    # Call Spotify API to get your data
    # ...

def upload_to_gcs():
    client = storage.Client()
    bucket = client.get_bucket('your_bucket_name')
    blob = bucket.blob('your_filename.csv')
    # Upload data to Google Storage
    # ...

with dag:
    t1 = PythonOperator(
        task_id='get_spotify_data',
        python_callable=get_spotify_data,
    )

    t2 = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=upload_to_gcs,
    )

    t1 >> t2

