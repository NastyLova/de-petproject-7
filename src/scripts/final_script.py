import os

from datetime import datetime
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# метод для записи данных в 2 target: в PostgreSQL для фидбэков и в Kafka для триггеров
def foreach_batch_function(df, epoch_id):
    # сохраняем df в памяти, чтобы не создавать df заново перед отправкой в Kafka
    df.persist()
    # записываем df в PostgreSQL с полем feedback
    df_topg = df.withColumn('feedback', None)
    (
        df_topg
        .write
        .outputMode("append")
        .format('jdbc')
        .option('url', 'jdbc:postgresql://localhost:5432/de')
        .option('driver', 'org.postgresql.Driver')
        .option('dbtable', 'public.subscribers_feedback')
        .option('user', 'jovyan')
        .option('password', 'jovyan')
        .save()
        )
    # создаём df для отправки в Kafka. Сериализация в json.
    df_tokafka = (
        df
        .withColumn('value', f.to_json(f.struct(f.col('restaurant_id'), 
                                                    f.col('adv_campaign_id'), 
                                                    f.col('adv_campaign_content'), 
                                                    f.col('adv_campaign_owner'), 
                                                    f.col('adv_campaign_owner_contact'), 
                                                    f.col('adv_campaign_datetime_start'), 
                                                    f.col('adv_campaign_datetime_end'), 
                                                    f.col('datetime_created'), 
                                                    f.col('client_id'), 
                                                    f.col('trigger_datetime_created'))))
    )
    # отправляем сообщения в результирующий топик Kafka без поля feedback
    (
        df_tokafka
        .writeStream
        .outputMode("append")
        .format("kafka")
        .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091')
        .options(**kafka_security_options)
        .option("topic", "student.topic.cohort15.anastasia_lovakova_out")
        .option("checkpointLocation", "test_query")
        .trigger(processingTime="15 seconds")
        .start()
        )
    # очищаем память от df
    df.unpersist()

# необходимые библиотеки для интеграции Spark с Kafka и PostgreSQL
spark_jars_packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            "org.postgresql:postgresql:42.4.0",
        ]
    )

kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";'
}

# создаём spark сессию с необходимыми библиотеками в spark_jars_packages для интеграции с Kafka и PostgreSQL
spark = SparkSession.builder \
    .appName("RestaurantSubscribeStreamingService") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.jars.packages", spark_jars_packages) \
    .getOrCreate()

# читаем из топика Kafka сообщения с акциями от ресторанов 
restaurant_read_stream_df = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091') \
    .option('kafka.security.protocol', 'SASL_SSL') \
    .option('kafka.sasl.jaas.config', 'org.apache.kafka.common.security.scram.ScramLoginModule required username="login" password="password";') \
    .option('kafka.sasl.mechanism', 'SCRAM-SHA-512') \
    .option('kafka.ssl.truststore.location', '/usr/lib/jvm/java-1.17.0-openjdk-amd64/lib/security/cacerts') \
    .option('kafka.ssl.truststore.password', 'changeit') \
    .option('subscribe', 'student.topic.cohort15.anastasia_lovakova_in') \
    .load()

# определяем схему входного сообщения для json
incomming_message_schema = StructType(
    [
        StructField("restaurant_id" , StringType(), True),
        StructField("adv_campaign_id", StringType(), True),
        StructField("adv_campaign_content" , StringType(), True),
        StructField("adv_campaign_owner" , StringType(), True),
        StructField("adv_campaign_owner_contact" , StringType(), True),
        StructField("adv_campaign_datetime_start" , DoubleType(), True),
        StructField("adv_campaign_datetime_end" , DoubleType(), True),
        StructField("datetime_created" , DoubleType(), True)
    ]
)

# определяем текущее время в UTC в миллисекундах
current_timestamp_utc = int(round(datetime.utcnow().timestamp()))

# десериализуем из value сообщения json и фильтруем по времени старта и окончания акции
filtered_read_stream_df = (
        restaurant_read_stream_df
        .withColumn('value', f.col('value').cast(StringType()))
        .withColumn('event', f.from_json(f.col('value'), incomming_message_schema))
        .selectExpr('event.*')
        .dropDuplicates(["restaurant_id", "adv_campaign_id", "adv_campaign_datetime_start", "adv_campaign_datetime_end"])
        .where(f.lit(current_timestamp_utc).between([f.col("adv_campaign_datetime_start"), f.col("adv_campaign_datetime_end")]))
        .withWatermark('timestamp', '10 minutes')
    )
# вычитываем всех пользователей с подпиской на рестораны
subscribers_restaurant_df = (
    spark.read
    .format('jdbc')
    .option('url', 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de')
    .option('driver', 'org.postgresql.Driver')
    .option('dbtable', 'subscribers_restaurants')
    .option('user', 'student')
    .option('password', 'de-student')
    .load()
)

# джойним данные из сообщения Kafka с пользователями подписки по restaurant_id (uuid). Добавляем время создания события.
fr = filtered_read_stream_df.alias('fr')
sr = subscribers_restaurant_df.alias('sr')
result_df = (
    fr.join(sr, 
            'fr.restaurant_id = sr.restaurant_id', 
            'inner')
    .select("sr.restaurant_id",
            "sr.adv_campaign_id",
            "sr.adv_campaign_content",
            "sr.adv_campaign_owner",
            "sr.adv_campaign_owner_contact",
            "sr.adv_campaign_datetime_start",
            "sr.adv_campaign_datetime_end",
            "sr.datetime_created",
            "fr.client_id",
            f.lit(current_timestamp_utc).alias("trigger_datetime_created")
            )
)

# запускаем стриминг
(
    result_df
    .writeStream
    .foreachBatch(foreach_batch_function)
    .start()
    .awaitTermination()
)