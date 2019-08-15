# Pub/Sub Indexing Service for Druid

> This is currently very much a WIP and some things might not yet work.

The Pub/Sub indexing service for [Druid](https://github.com/apache/incubator-druid) enables ingestion from [Cloud Pub/Sub](https://cloud.google.com/pubsub/). It works similar to the existing [Kafka indexing service](https://druid.apache.org/docs/latest/development/extensions-core/kafka-ingestion) or [Kinesis indexing service](https://druid.apache.org/docs/latest/development/extensions-core/kinesis-ingestion.html). However, there are some differences between Kafka/Kinesis and Pub/Sub that make it impossible to simply reuse the existing services:
* Kafka is a streaming log while Pub/Sub is a message log
* Kafka requires offset management
* Ingesting messages from Pub/Sub is similar to batch ingestion while Kafka is more similar to stream ingestion
* Message order cannot be determined in Pub/Sub
* Pub/Sub is sending a message _at least once_, so there might be duplicates
* In Pub/Sub a message gets repeatedly sent to the consumer until it gets acknowledged

## Building

Run `mvn package` to create a `.jar` file that contains all dependencies.

The generated `.jar` file needs to be copied into druids extensions/ directory (for example into `/app/druid/extensions/pubsu-indexing-service/`).

When running druid it should automatically detect and execute this extension.

## Issues and Troubleshooting

* It is not easily possible to use [Java GCP client libraries](https://github.com/googleapis/google-cloud-java). Druid depends on Guava 16.0.1 while the GCP libraries use the most recent version of Guava for accessing APIs via gRPC. Attempting to use GCP libraries withing Druid or in extensions will result in many `java.lang.NoSuchMethodError` exceptions since Guava 16 will be used in the GCP libraries. That version is very old and does not implement certain methods required to run the GCP libraries. For this reason, this project only uses the Pub/Sub REST API for pulling messages. This method might not be as performant as using gRPC.
* Druid recognizes the indexing service as a new extension, however sometimes the `DruidModule` is not initialized causing the indexing service not to get executed. This seems to happen randomly.

## Alternatives

Instead of using a custom indexing service, an alternative is to extend [tranquility](https://github.com/druid-io/tranquility) and add Pub/Sub support. A fork implementing this can be found here: [https://github.com/scholtzan/tranquility](https://github.com/scholtzan/tranquility)
