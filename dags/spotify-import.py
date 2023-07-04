from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from airflow.hooks.base_hook import BaseHook
from operators.bigquery_operator import BigQueryOperator


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

    conn = BaseHook.get_connection('spotify_conn')
    spotify_client_id = conn.login
    spotify_client_secret = conn.password
    
    client_credentials_manager = SpotifyClientCredentials(
        client_id=spotify_client_id,
        client_secret=spotify_client_secret,
    )

    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    results = sp.search(q='artist:Ed Sheeran', type='track', limit=10)
    for track in results['tracks']['items']:
        print(track['name'])

def upload_to_gcs():
    # client = storage.Client()
    # bucket = client.get_bucket('your_bucket_name')
    # blob = bucket.blob('your_filename.csv')
    # Upload data to Google Storage
    # ...
    print("do nothing for now")

    data = [
        {'column1': 'value1', 'column2': 1},
        {'column1': 'value3', 'column2': 3},
    ]
    return BigQueryOperator(data)

with dag:
    t1 = PythonOperator(
        task_id='get_spotify_data',
        python_callable=get_spotify_data,
    )

    t2 = BigQueryOperator(
        task_id='upload_to_gcs',
        data = [
            {'column1': 'value1', 'column2': 1},
            {'column1': 'value3', 'column2': 3},
        ]
    )

    t1 >> t2

