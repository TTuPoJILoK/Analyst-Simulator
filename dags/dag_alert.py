import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import telegram
import pandahouse as ph
from datetime import date, datetime, timedelta
import io
from airflow.decorators import dag, task


def check_anomaly(df, metric, a=4, n=5):
    # Проверяем аномалию с помощью межквартильного размаха
    df['q25'] = df[metric].shift(1).rolling(n).quantile(0.25)
    df['q75'] = df[metric].shift(1).rolling(n).quantile(0.75)
    df['iqr'] = df['q75'] - df['q25']
    df['up'] = df['q75'] + a * df['iqr']
    df['low'] = df['q25'] - a * df['iqr']
    # Сгладим окно
    df['up'] = df['up'].rolling(n, center=True, min_periods=1).mean()
    df['low'] = df['low'].rolling(n, center=True, min_periods=1).mean()
    # Проверка на вылкт за пределы окна
    if df[metric].iloc[-1] < df['low'].iloc[-1] or df[metric].iloc[-1] > df['up'].iloc[-1]:
        is_alert = 1
    else:
        is_alert = 0

    return is_alert, df

default_args = {
    'owner': 'o-haljavin',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 8, 9)
}

schedule_interval = '*/15 * * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_alert_khalyvin_15963():
    
    @task
    def run_alerts(chat=None):
        # Подключаемся к боту
        chat_id = chat or 357651353
        bot = telegram.Bot(token='6550935641:AAEZrWnDRswv9wR4oOgrkPdoip8EmqqhXz0')
        # Извлекаекм нужные метрики
        connection = {'host': 'https://clickhouse.lab.karpov.courses',
                          'database':'simulator_20230720',
                          'user':'student', 
                          'password':'dpo_python_2020'
                         }
        
        q = ''' SELECT ts, date, hm, users_lenta, users_messendger, likes, views, ctr, messages
            FROM (
            SELECT   toStartOfFifteenMinutes(time) as ts
                                    , toDate(ts) as date
                                    , formatDateTime(ts, '%R') as hm
                                    , uniqExact(user_id) as users_lenta
                                    , countIf(user_id, action='like') as likes
                                    , countIf(user_id, action='view') as views
                                    , round(likes / views, 3) as ctr
            FROM simulator_20230720.feed_actions
            WHERE ts >=  today() - 1 and ts < toStartOfFifteenMinutes(now())
            GROUP BY ts, date, hm
            ORDER BY ts ) t
            JOIN (
            SELECT   toStartOfFifteenMinutes(time) as ts
                                    , toDate(ts) as date
                                    , formatDateTime(ts, '%R') as hm
                                    , uniqExact(user_id) as users_messendger
                                    , count(user_id) as messages
            FROM simulator_20230720.message_actions
            WHERE ts >=  today() - 1 and ts < toStartOfFifteenMinutes(now())
            GROUP BY ts, date, hm
            ORDER BY ts
            ) t1 using (ts)'''
        
        data = ph.read_clickhouse(q, connection=connection)
    
        metrics_list = ['users_lenta', 'likes', 'views', 'ctr', 'messages', 'users_messendger']
        
        for metric in metrics_list:
            df = data[['ts', 'date', 'hm', metric]].copy()
            # проверяем метрику на аномальность алгоритмом, описаным внутри функции check_anomaly()
            is_alert, df = check_anomaly(df, metric) 
        
            if is_alert == 1:
                current_value=df[metric].iloc[-1],
                diff=abs(1-(df[metric].iloc[-1]/df[metric].iloc[-2]))
                # Формируем сообщение с предупреждением
                msg = f'''
                ⚠️⚠️⚠️ALERT⚠️⚠️⚠️\nМетрика {metric}:\nтекущее значение  {current_value[0]},\nотклонение от предыдущего значения  {round(diff, 2)},\nДашборд: https://superset.lab.karpov.courses/superset/dashboard/4082
                '''
    
                # Строим график
                sns.set(rc={'figure.figsize': (16, 10)}) # задаем размер графика
                plt.tight_layout()
        
                ax = sns.lineplot(x=df['hm'], y=df[metric], label=metric)
                ax = sns.lineplot(x=df['hm'], y=df['up'], label='up')
                ax = sns.lineplot(x=df['hm'], y=df['low'], label='low')
        
                for ind, label in enumerate(ax.get_xticklabels()): # этот цикл нужен чтобы разрядить подписи координат по оси Х,
                    if ind % 15 == 0:
                        label.set_visible(True)
                    else:
                        label.set_visible(False)
                ax.set(xlabel='time') # задаем имя оси Х
                ax.set(ylabel=metric) # задаем имя оси У
        
                ax.set_title('{}'.format(metric)) # задаем заголовок графика
                ax.set(ylim=(0, None)) # задаем лимит для оси У
        
                # формируем файловый объект
                plot_object = io.BytesIO()
                ax.figure.savefig(plot_object)
                plot_object.seek(0)
                plot_object.name = '{0}.png'.format(metric)
                plt.close()
        
                # отправляем алерт
                bot.sendMessage(chat_id=chat_id, text=msg,)
                bot.sendPhoto(chat_id=chat_id, photo=plot_object)
                
    run_alerts()
    
dag_alert_khalyvin_15963 = dag_alert_khalyvin_15963()