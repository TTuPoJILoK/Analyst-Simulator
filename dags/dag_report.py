import telegram
import matplotlib.pyplot as plt
import seaborn as sns
import io
from datetime import date, datetime, timedelta
import pandas as pd
from airflow.decorators import dag, task
import pandahouse as ph


default_args = {
    'owner': 'o-haljavin',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 8, 11)
}

schedule_interval = '0 11 * * *'
    
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_report_khalyvin_15963():
        
    @task()
    def last_week_data():
        
        connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'simulator_20230720',
                      'user':'student', 
                      'password':'dpo_python_2020'
                     }
        
        q = '''
        SELECT toStartOfDay(toDateTime(time)) AS date,
               count(DISTINCT user_id) AS dau,
               countIf(action='like') AS likes,
               countIf(action='view') AS views,
               countIf(user_id, action = 'like') / countIf(user_id, action = 'view') AS ctr
        FROM simulator_20230720.feed_actions
        where date > today() - 8 and date <= yesterday()
        GROUP BY date
        order by date
        '''
        
        data_week = ph.read_clickhouse(q, connection=connection)
        
        return data_week
        
    @task()
    def send_message(df):
        my_token = '6550935641:AAEZrWnDRswv9wR4oOgrkPdoip8EmqqhXz0' 
        bot = telegram.Bot(token=my_token) 
        chat_id = 357651353
        
        yesterday = datetime.now().date() - timedelta(1)
        
        msg = f"Статистика за вчерашний день: \nDAU: {list(df[df['date'] == str(yesterday)]['dau'])[0]} \nЛайки: {list(df[df['date'] == str(yesterday)]['likes'])[0]} \nПросмотры: {list(df[df['date'] == str(yesterday)]['views'])[0]}\nCTR: {round(list(df[df['date'] == str(yesterday)]['ctr'])[0], 3)}"
        bot.sendMessage(chat_id=chat_id, text=msg)
        
    @task()
    def send_picture(df):
        my_token = '6550935641:AAEZrWnDRswv9wR4oOgrkPdoip8EmqqhXz0' 
        bot = telegram.Bot(token=my_token) 
        chat_id = 357651353
        
        fig, ax = plt.subplots(2, 2)
        plt.figure(figsize=(20,14))
        
        plt.subplot(2, 2, 1)
        ax = sns.lineplot(x=df['date'], y=df['dau']).set(xlabel=None)
        plt.title('DAU за прошлую неделю')
        
        
        plt.subplot(2, 2, 2)
        ax = sns.lineplot(x=df['date'], y=df['likes']).set(xlabel=None)
        plt.title('Лайки за прошлую неделю')
        
        plt.subplot(2, 2, 3)
        ax = sns.lineplot(x=df['date'], y=df['views']).set(xlabel=None)
        plt.title('Просмотры за прошлую неделю')
        plot_object = io.BytesIO()
        
        
        plt.subplot(2, 2, 4)
        ax = sns.lineplot(x=df['date'], y=df['ctr']).set(xlabel=None)
        plt.title('CTR за прошлую неделю')
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'test_plot.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        
        
    df = last_week_data()
    send_message(df)
    send_picture(df)
    
    
dag_report_khalyvin_15963 = dag_report_khalyvin_15963()
