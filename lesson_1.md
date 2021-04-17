
## 1. Подключаемся к серверу

```bash
ssh 305_kruglikov@185.241.193.174 -i ~/.ssh/id_rsa_gb_spark
```

## 2. Запускаем спарк-приложение
- дл работы только на одном воркере

```bash
/spark2.4/bin/pyspark --master local
```
- или для работы на всем кластере

```bash
pyspark
```
<details>
    <summary>вывод консоли</summary>

```bash
Python 2.7.5 (default, Apr  2 2020, 13:16:51)
[GCC 4.8.5 20150623 (Red Hat 4.8.5-39)] on linux2
Type "help", "copyright", "credits" or "license" for more information.
Warning: Ignoring non-Spark config property: hive.metastore.uris
21/04/17 11:49:13 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
21/04/17 11:49:15 WARN util.Utils: Service 'SparkUI' could not bind on port 4040. Attempting port 4041.
21/04/17 11:49:15 WARN util.Utils: Service 'SparkUI' could not bind on port 4041. Attempting port 4042.
21/04/17 11:49:15 WARN util.Utils: Service 'SparkUI' could not bind on port 4042. Attempting port 4043.
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 2.4.7
      /_/

Using Python version 2.7.5 (default, Apr  2 2020 13:16:51)
SparkSession available as 'spark'.

```
</details>

## 3. Далее пробуем выполнить команды из файла

### Создаем спарк-сессию

```python
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("gogin_spark").getOrCreate()
```

__NB!__ Держим в голове схему SOURCE -> PROCESSING -> SINK

#### Rate SOURCE

```python
raw_rate = spark.readStream.format("rate").load()
raw_rate.printSchema()  # просмотр схемы датафрейма
```
<details>
    <summary>вывод консоли</summary>
    
```bash
root
 |-- timestamp: timestamp (nullable = true)
 |-- value: long (nullable = true)
```
</details>

```python
# raw_rate.show()  # не сработает
```

#### проверяем, что все работает

```python
raw_rate.isStreaming  # проверяем, что у датафрейма есть специальный параметр
```
<details>
    <summary>вывод консоли</summary>
    
```bash
True
```
</details>

#### запускаем стрим на консоль
```python
stream = raw_rate.writeStream \
    .format("console") \
    .start()  # побежит быстро

stream.stop()
```
<details>
    <summary>вывод консоли</summary>
    
```bash
>>> -------------------------------------------
Batch: 0
-------------------------------------------
+---------+-----+
|timestamp|value|
+---------+-----+
+---------+-----+

-------------------------------------------
Batch: 1
-------------------------------------------
+--------------------+-----+
|           timestamp|value|
+--------------------+-----+
|2021-04-17 12:02:...|    0|
+--------------------+-----+

-------------------------------------------
Batch: 2
-------------------------------------------
+--------------------+-----+
|           timestamp|value|
+--------------------+-----+
|2021-04-17 12:02:...|    1|
|2021-04-17 12:02:...|    2|
|2021-04-17 12:02:...|    3|
+--------------------+-----+

```
</details>

#### запускаем медленно
```python
stream = raw_rate \
    .writeStream \
    .trigger(processingTime='30 seconds') \
    .format("console") \
    .options(truncate=False) \
    .start()
```
<details>
    <summary>вывод консоли</summary>
    
```bash
-------------------------------------------
+---------+-----+
|timestamp|value|
+---------+-----+
+---------+-----+

-------------------------------------------
Batch: 1
-------------------------------------------
+-----------------------+-----+
|timestamp              |value|
+-----------------------+-----+
|2021-04-17 12:06:10.794|0    |
|2021-04-17 12:06:11.794|1    |
|2021-04-17 12:06:12.794|2    |
|2021-04-17 12:06:13.794|3    |
|2021-04-17 12:06:14.794|4    |
|2021-04-17 12:06:15.794|5    |
|2021-04-17 12:06:16.794|6    |
|2021-04-17 12:06:17.794|7    |
|2021-04-17 12:06:18.794|8    |
|2021-04-17 12:06:19.794|9    |
|2021-04-17 12:06:20.794|10   |
|2021-04-17 12:06:21.794|11   |
|2021-04-17 12:06:22.794|12   |
|2021-04-17 12:06:23.794|13   |
|2021-04-17 12:06:24.794|14   |
|2021-04-17 12:06:25.794|15   |
|2021-04-17 12:06:26.794|16   |
|2021-04-17 12:06:27.794|17   |
|2021-04-17 12:06:28.794|18   |
+-----------------------+-----+
```
</details>

#### Проверяем параметры

##### Оператор EXPLAIN используется для предоставления логических / физических планов для оператора ввода. По умолчанию этот пункт предоставляет информацию только о физическом плане.
```python
stream.explain()
```
<details>
    <summary>вывод консоли</summary>
    
```bash
== Physical Plan ==
WriteToDataSourceV2 org.apache.spark.sql.execution.streaming.sources.MicroBatchWriter@5a44866d
+- Scan ExistingRDD[timestamp#301,value#302L]: 1
```
</details>

##### Возвращает `True`, если этот запрос активно выполняется.

```python
stream.isActive
```
<details>
    <summary>вывод консоли</summary>
    
```bash
True
```
</details>

##### Возвращает самое последнее обновление `StreamingQueryProgress` для этого потокового запроса.
```python
from pprint import pprint
pprint(stream.lastProgress)
```
<details>
    <summary>вывод консоли</summary>
    
```bash
{u'batchId': 17,
 u'durationMs': {u'addBatch': 71,
                 u'getBatch': 9,
                 u'getOffset': 0,
                 u'queryPlanning': 6,
                 u'triggerExecution': 143,
                 u'walCommit': 55},
 u'id': u'4c302ffb-10d0-4025-a5df-84fa4b069d06',
 u'inputRowsPerSecond': 1.0,
 u'name': None,
 u'numInputRows': 30,
 u'processedRowsPerSecond': 209.79020979020981,
 u'runId': u'b9b452a5-4de6-4a6d-bf4b-82b1adf786b4',
 u'sink': {u'description': u'org.apache.spark.sql.execution.streaming.ConsoleSinkProvider@10d4ccf'},
 u'sources': [{u'description': u'RateSource[rowsPerSecond=1, rampUpTimeSeconds=0, numPartitions=2]',
               u'endOffset': 499,
               u'inputRowsPerSecond': 1.0,
               u'numInputRows': 30,
               u'processedRowsPerSecond': 209.79020979020981,
               u'startOffset': 469}],
 u'stateOperators': [],
 u'timestamp': u'2021-03-06T14:14:30.000Z'}
```
</details>

##### Возвращает текущий статус запроса.
```python
pprint(stream.status)
stream.stop()
```
<details>
    <summary>вывод консоли</summary>
    
```bash
{u'isDataAvailable': True,
 u'isTriggerActive': False,
 u'message': u'Waiting for next trigger'}
```
</details>

#### Функция, чтобы выводить на консоль, вместо `show()`
```python
def console_output(df, freq):
    return df.writeStream \
                .format("console") \
                .trigger(processingTime='%s seconds' % freq) \
                .options(truncate=False) \
                .start()

out = console_output(raw_rate, 5)
out.stop()
```
<details>
    <summary>вывод консоли</summary>
    
```bash

Batch: 0
-------------------------------------------
+---------+-----+
|timestamp|value|
+---------+-----+
+---------+-----+

-------------------------------------------
Batch: 1
-------------------------------------------
+-----------------------+-----+
|timestamp              |value|
+-----------------------+-----+
|2021-04-17 12:25:55.793|0    |
|2021-04-17 12:25:56.793|1    |
|2021-04-17 12:25:57.793|2    |
|2021-04-17 12:25:58.793|3    |
+-----------------------+-----+

-------------------------------------------
Batch: 2
-------------------------------------------
+-----------------------+-----+
|timestamp              |value|
+-----------------------+-----+
|2021-04-17 12:25:59.793|4    |
|2021-04-17 12:26:00.793|5    |
|2021-04-17 12:26:01.793|6    |
|2021-04-17 12:26:02.793|7    |
|2021-04-17 12:26:03.793|8    |
+-----------------------+-----+
```
</details>

#### добавляем собственный фильтр
```python
filtered_rate = raw_rate \
    .filter(F.col("value") % F.lit("2") == 0)

out = console_output(filtered_rate, 5)
out.stop()
```
<details>
    <summary>вывод консоли</summary>
    
```bash
Batch: 1
-------------------------------------------
+----------------------+-----+
|timestamp             |value|
+----------------------+-----+
|2021-04-17 12:31:44.45|0    |
|2021-04-17 12:31:46.45|2    |
|2021-04-17 12:31:48.45|4    |
+----------------------+-----+

-------------------------------------------
Batch: 2
-------------------------------------------
+----------------------+-----+
|timestamp             |value|
+----------------------+-----+
|2021-04-17 12:31:50.45|6    |
|2021-04-17 12:31:52.45|8    |
+----------------------+-----+
```
</details>

#### добавляем собственные колонки
```python
extra_rate = filtered_rate.withColumn(
    "my_value", F.when(
        (F.col("value") % F.lit(10) == 0),
        F.lit("jubilee")).otherwise(F.lit("not yet")))

out = console_output(extra_rate, 5)
out.stop()
```
<details>
    <summary>вывод консоли</summary>
    
```bash
Batch: 1
-------------------------------------------
+-----------------------+-----+--------+
|timestamp              |value|my_value|
+-----------------------+-----+--------+
|2021-04-17 12:39:28.637|0    |jubilee |
+-----------------------+-----+--------+

-------------------------------------------
Batch: 2
-------------------------------------------
+-----------------------+-----+--------+
|timestamp              |value|my_value|
+-----------------------+-----+--------+
|2021-04-17 12:39:30.637|2    |not yet |
|2021-04-17 12:39:32.637|4    |not yet |
+-----------------------+-----+--------+

-------------------------------------------
Batch: 3
-------------------------------------------
+-----------------------+-----+--------+
|timestamp              |value|my_value|
+-----------------------+-----+--------+
|2021-04-17 12:39:34.637|6    |not yet |
|2021-04-17 12:39:36.637|8    |not yet |
|2021-04-17 12:39:38.637|10   |jubilee |
+-----------------------+-----+--------+
```
</details>

### Eсли потеряем стрим из переменной, сможем остановить все наши стримы, получив их из спарк окружения
```python
def killAll():
    for active_stream in spark.streams.active:
        print("Stopping %s by killAll" % active_stream)
        active_stream.stop()
        
killAll()
```

---
Все работает!!!


```python

```
