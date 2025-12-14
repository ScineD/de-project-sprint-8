# Хранилище данных

## Структура

**Исходники:** `/user/master/data/geo/events/date=YYYY-MM-DD` - события в parquet

**Справочники:** `/user/scined/project/dicts/geo.csv` - города с координатами

**Результаты:** `/user/scined/project/analytics/` - витрины в parquet:

- **table_1** - географический профиль юзеров (каждые 90 дней): user_id, act_city, home_city, travel_count, travel_array, local_time
- **table_2** - статистика по городам (каждые 30 дней): month, week, zone_id, week_*/month_* метрики
- **table_3** - рекомендации по пэйрингу (каждый 1 день): user_left, user_right, processed_dttm, zone_id, local_time

## Запуск

Вручную через airflow, dag файлы: `dag_table_1.py`, `dag_table_2.py`, `dag_table_3.py`

Или напрямую:
```bash
spark-submit table_1.py 2022-06-21 90 /user/master/data/geo/events/date
spark-submit table_2.py 2022-05-15 30 /user/master/data/geo/events/date
spark-submit table_3.py 2022-05-21 1 /user/master/data/geo/events/date
```

Аргументы: дата, глубина_в_днях, путь_к_данным

## Конфиг

- формат parquet, overwrite при каждом запуске
- логи ищите по `====[TABLE_X]====` в airflow UI
