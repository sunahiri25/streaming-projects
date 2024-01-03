# Real-time Sentiment Analysis on Yelp Reviews

## System Architecture

![architecture](https://github.com/sunahiri25/streaming-projects/blob/main/public/System_architecture.png)

### The project is designed with the following components:

- Data Source: We use [yelp.com](https://www.yelp.com/dataset) dataset for our pipeline.
- TCP/IP Socket: Used to stream data over the network in chunks
- Apache Spark: For data processing with its master and worker nodes.
- ML model: For analysing customer reviews
- Confluent Kafka: Our cluster on the cloud
- Control Center and Schema Registry: Helps in monitoring and schema management of our Kafka streams.
- Kafka Connect: For connecting to elasticsearch
- Elasticsearch: For indexing and querying
- Kibana: For visualizing

## Technologies
- Python
- TCP/IP
- Confluent Kafka
- Apache Spark
- Docker
- Elasticsearch
- Kibana
