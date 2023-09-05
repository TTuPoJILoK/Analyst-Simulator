# Analyst-Simulator
### Решение задач по курсу [симулятор аналитика](https://karpov.courses/simulator) от karpov.courses
#### Задача состояла в выстраивании с нуля аналитических процессов в стартапе, объединившем мессенджер и ленту новостей.
---
Поэтапоно работа выглядела следующим образом:
1. Построение дашбордов
   - Основные метрики ленты новостей [за все время](dashboards/feed.gif) и [за последний день](dashboards/feed_realtime.gif)
   - [Взаимодействие ленты новостей и мессенджера](dashboards/feed_and_messenger.gif)
2. Анализ продуктовых метрик
   - [Retention пользователей с разным трафиком](product_metrics/retention)
   - [Аномалия новых пользователей](product_metrics/retention)
   - [Просадка активных пользователей](product_metrics/loss)
3. A/B-тестирование
   - [A/A-тестирование](./AB_Test/aa_test.ipynb)
   - [Методы проверок гипотез](./AB_Test/ab_test.ipynb)
   - [Линеаризованные метрики](./AB_Test/ab_test_linearized.ipynb)
4. Прогнозирование метрик
   - [Анализ результатов флешмоба в ленте](predict_metrics/flahmob_analysis.ipynb)
   - [Предсказание активности аудитории](predict_metrics/predict_activity.ipynb)
6. [ETL](dags)
   - Построение ETL пайплайна
   - Автоматизация отчётности
   - Система алертов

---

### Стек технологий
- SQL:            Clickhouse, Redash
- Python:         Pandas, Airflow, Seaborn, Numpy
- Визуализация:   Superset
---

![сертификат](certificate.png)
