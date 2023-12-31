from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag,task
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from udacity.common.final_project_sql_statements import SqlQueries
from airflow.operators.postgres_operator import PostgresOperator
from airflow.models import Variable

s3_bucket = Variable.get('s3_bucket')
s3_prefix = Variable.get('s3_prefix')


default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    
        
    
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id="Stage_events",
        table="staging_events",
        redshift_conn_id="redshift",
        aws_credentials_id="dylamug",
        s3_bucket="dylabucket",
        s3_key="log-data/",
        log_json_file="log_json_path.json"
    )


    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        table="staging_songs",
        redshift_conn_id="redshift",
        aws_credentials_id="dylamug",
        s3_bucket="dylabucket",
        s3_key="song-data/",
        log_json_file=""

    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        table="songplays",
        redshift_conn_id="redshift",
        aws_credentials_id="dylamug",
        sql_statement=SqlQueries.songplay_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        table="users",
        redshift_conn_id="redshift",
        aws_credentials_id="dylamug",
        sql_statement=SqlQueries.user_table_insert,
        append_only=False
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        table="songs",
        redshift_conn_id="redshift",
        aws_credentials_id="dylamug",
        sql_statement=SqlQueries.song_table_insert,
        append_only=False
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        table="artists",
        redshift_conn_id="redshift",
        aws_credentials_id="dylamug",
        sql_statement=SqlQueries.artist_table_insert,
        append_only=False
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        table="time",
        redshift_conn_id="redshift",
        aws_credentials_id="dylamug",
        sql_statement=SqlQueries.time_table_insert,
        append_only=False
    )
    tests = [
   
    # users
    {'check_sql': 'SELECT COUNT(*) FROM users WHERE user_id IS NULL', 'expected_result': 0},
    {'check_sql': 'SELECT COUNT(*) FROM users WHERE first_name IS NULL AND last_name IS NULL', 'expected_result': 0},    
    # songs
    {'check_sql': 'SELECT COUNT(*) FROM songs WHERE song_id IS NULL', 'expected_result': 0},
    {'check_sql': 'SELECT COUNT(*) FROM songs WHERE title IS NULL', 'expected_result': 0},    
    # artists
    {'check_sql': 'SELECT COUNT(*) FROM artists WHERE artist_id IS NULL', 'expected_result': 0},
    {'check_sql': 'SELECT COUNT(*) FROM artists WHERE artist_name IS NULL', 'expected_result': 0},    
    # time
    {'check_sql': 'SELECT COUNT(*) FROM time WHERE start_time IS NULL', 'expected_result': 0},
    {'check_sql': 'SELECT COUNT(DISTINCT hour) FROM time', 'expected_result': 24},
    ]
    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id="redshift",
        tests = tests
    )
    
    end_operator = DummyOperator(task_id='End_execution')

    
    start_operator >> stage_events_to_redshift
    start_operator >> stage_songs_to_redshift
    stage_events_to_redshift >> load_songplays_table
    stage_songs_to_redshift >> load_songplays_table
    load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] \
    >> run_quality_checks \
    >> end_operator
final_project_dag = final_project()
