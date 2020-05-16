### Расчет метрик Installs, Retention, LTV

**Установка зависимостей**
```
pipenv shell
pipenv install
```

**Описание опций командной строки**
```
python clickhouse.py -h
```

**Создание БД, таблиц и запись данных**
```
python clickhouse.py --password <ch_user_password> --db <db_name> -o insert -n <records_num>
```

**Запуск процедуры**
```
python clickhouse.py --password <ch_user_password> --db <db_name> -s <period_from> -e <period_to> -r <ref_pattern> -o <procedure_name>
```