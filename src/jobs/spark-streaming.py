import time

import pandas as pd
from config.config import config
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf, when
from pyspark.sql.types import StructField, StructType, StringType, FloatType
import numpy as np
from sklearn.svm import LinearSVC
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import TfidfVectorizer


def sentiment_analysis(comment) -> str:
    if comment:
        X = vectorizer.transform([comment])
        svc_predictions = svc_classifier.predict(X)[0]
        return "Positive" if svc_predictions == 1 else "Negative"
    return "Empty"


def start_streaming(spark):
    topic = "customer_reviews"
    while True:
        try:
            stream_df = (spark.readStream.format("socket")
                         .option("host", "0.0.0.0")
                         .option("port", 9999)
                         .load())
            schema = StructType([
                StructField("review_id", StringType()),
                StructField("user_id", StringType()),
                StructField("business_id", StringType()),
                StructField("stars", FloatType()),
                StructField("date", StringType()),
                StructField("text", StringType()),
            ])

            stream_df = stream_df.select(from_json(col("value"), schema).alias("data")).select(("data.*"))

            # query = stream_df.writeStream.outputMode("append").format("console").options(truncate=False).start()
            # query.awaitTermination()

            sentiment_analysis_udf = udf(sentiment_analysis, StringType())
            stream_df = stream_df.withColumn('feedback',
                                             when(col('text').isNotNull(),
                                                  sentiment_analysis_udf(col('text'))).otherwise(None))

            kafka_df = stream_df.selectExpr("CAST(review_id AS STRING) AS key", "to_json(struct(*)) AS value")
            query = (kafka_df.writeStream
                     .format("kafka")
                     .option("kafka.bootstrap.servers", config['kafka']['bootstrap.servers'])
                     .option("kafka.security.protocol", config['kafka']['security.protocol'])
                     .option('kafka.sasl.mechanism', config['kafka']['sasl.mechanism'])
                     .option('kafka.sasl.jaas.config',
                             'org.apache.kafka.common.security.plain.PlainLoginModule required username="{username}" '
                             'password="{password}";'.format(
                                 username=config['kafka']['sasl.username'],
                                 password=config['kafka']['sasl.password']
                             ))
                     .option('checkpointLocation', '/tmp/checkpoint')
                     .option('topic', topic)
                     .start()
                     .awaitTermination()
                     )

        except Exception as e:
            print(f'Exception: {e}. Retrying in 10 seconds...')
            time.sleep(10)


if __name__ == "__main__":
    spark_conn = SparkSession.builder.appName("SocketStreamConsumer").getOrCreate()

    # Loading and preprocess
    train = pd.read_csv('datasets/train.csv', names=('class', 'text'))
    test = pd.read_csv('datasets/test.csv', names=('class', 'text'))
    train.loc[train["class"] == 1, "class"] = 0
    train.loc[train["class"] == 2, "class"] = 1
    test.loc[test["class"] == 1, "class"] = 0
    test.loc[test["class"] == 2, "class"] = 1
    svc_texts = list(train['text'])
    svc_labels = list(train['class'])
    # Feature extraction
    vectorizer = TfidfVectorizer(ngram_range=(1, 2), min_df=3)
    Xs = vectorizer.fit_transform(svc_texts)

    # Spliting data
    Xs_train, Xs_test, ys_train, ys_test = train_test_split(Xs, np.array(svc_labels), test_size=0.4)

    # Model initialization and trainng
    svc_classifier = LinearSVC(max_iter=5000)
    svc_classifier.fit(Xs_train, ys_train)

    start_streaming(spark_conn)
