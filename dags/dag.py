import pandas as pd
import pandahouse as ph
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


default_args = {
    'owner': 'o-haljavin',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 8, 1),
}

schedule_interval = '0 21 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_etl_khalyvin_15963():
    
    @task()
    def extract_feed():
        connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'simulator_20230720',
                      'user':'student', 
                      'password':'dpo_python_2020'
                     }
        q = '''
        SELECT
          toDate(time) as event_date,
          user_id,
          countIf(action = 'like') likes,
          countIf(action = 'view') views,
          gender,
          os,
          age
        FROM
          simulator_20230720.feed_actions
        GROUP BY
          event_date,
          user_id,
          gender,
          os,
          age      
        '''
    
        df = ph.read_clickhouse(q, connection=connection)
        return df
    
    @task
    def extract_messages():
        connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'simulator_20230720',
                      'user':'student', 
                      'password':'dpo_python_2020'
                     }
        q = '''
        SELECT
          event_date,
          user_id,
          messages_sent,
          users_sent,
          messages_received,
          users_received,
          gender,
          os,
          age
        FROM(
            SELECT
              user_id,
              toDate(time) event_date,
              count(reciever_id) messages_sent,
              count(distinct reciever_id) users_sent,
              gender,
              os,
              age
            FROM
              simulator_20230720.message_actions
            WHERE
              toDate(time) = yesterday()
            GROUP BY
              event_date,
              user_id,
              gender,
              os,
              age
          ) t FULL
          JOIN (
            SELECT
              reciever_id,
              toDate(time) event_date,
              count(user_id) messages_received,
              count(distinct user_id) users_received,
              gender,
              os,
              age
            FROM
              simulator_20230720.message_actions
            WHERE
              toDate(time) = yesterday()
            GROUP BY
              event_date,
              reciever_id,
              gender,
              os,
              age
          ) t1 on user_id = reciever_id
          WHERE event_date != '1970-01-01'
        '''
    
        df = ph.read_clickhouse(q, connection=connection)
        return df

    @task
    def merge_dfs(df1, df2):
        df = df1.merge(df2, on=['event_date', 'user_id', 'gender', 'age', 'os'], how='outer').dropna().reset_index()
        return df[['event_date', 'user_id', 'likes', 'views', 'gender', 'os', 'age', 'messages_sent',        'users_sent','messages_received', 'users_received']]
    
    @task
    def transfrom_gender(df):
        df_gender = df[['event_date', 'gender', 'likes', 'views', 'messages_sent', 'users_sent', 'messages_received', 'users_received']]\
            .groupby(['event_date', 'gender'])\
            .sum()\
            .reset_index()
        df_gender['dimension'] = 'gender'
        df_gender = df_gender.rename(columns={'gender': 'dimension_value'})
        return df_gender
    
    @task
    def transfrom_os(df):
        df_os = df[['event_date', 'os', 'likes', 'views', 'messages_sent', 'users_sent', 'messages_received', 'users_received']]\
            .groupby(['event_date', 'os'])\
            .sum()\
            .reset_index()
        df_os['dimension'] = 'os'
        df_os = df_os.rename(columns={'os': 'dimension_value'})
        return df_os
    
    @task
    def transfrom_age(df):
        df_age = df[['event_date', 'age', 'likes', 'views', 'messages_sent', 'users_sent', 'messages_received', 'users_received']]\
            .groupby(['event_date', 'age'])\
            .sum()\
            .reset_index()
        df_age['dimension'] = 'age'
        df_age = df_age.rename(columns={'age': 'dimension_value'})
        return df_age
    
    @task
    def load(df_age, df_os, df_gender):
        df_res = pd.concat([df_os, df_age, df_gender], ignore_index=True)
        df_res = df_res[['event_date', 'dimension', 'dimension_value', 'views', 'likes', 'messages_received', 
                                         'messages_sent', 'users_received', 'users_sent']]
        df_res = df_res.astype({'views':'int64', 'likes':'int64', 'messages_received':'int64', 'messages_sent':'int64', 
                                                'users_received':'int64', 'users_sent':'int64'})
    
    
      
        connection = {'host': 'https://clickhouse.lab.karpov.courses',
                          'database':'test',
                          'user':'student-rw', 
                          'password':'656e2b0c9c'
                         }
        q = '''
            CREATE TABLE IF NOT EXISTS test.khalyvin_etl_dag
                (
                event_date Date,
                dimension String,
                dimension_value String,
                views UInt64,
                likes UInt64,
                messages_received UInt64,
                messages_sent UInt64,
                users_received UInt64,
                users_sent UInt64
                )
                ENGINE = MergeTree()
                ORDER BY event_date
            '''
        ph.execute(q, connection=connection)
        ph.to_clickhouse(df=df_res, table='khalyvin_etl_dag', index=False, connection=connection)
            
    df_feed = extract_feed()
    df_mes = extract_messages()
    df_all = merge_dfs(df_feed, df_mes)
    df_gender = transfrom_gender(df_all)
    df_os = transfrom_os(df_all)
    df_age = transfrom_age(df_all)
    load(df_gender, df_os, df_age)
    
dag_etl_khalyvin_15963 = dag_etl_khalyvin_15963()