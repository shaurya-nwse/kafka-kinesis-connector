Kafka connector for Kinesis
===

[![Develop](https://github.com/shaurya-nwse/kafka-kinesis-connector/actions/workflows/build.yml/badge.svg?branch=develop)](https://github.com/shaurya-nwse/kafka-kinesis-connector/actions/workflows/build.yml)

Kafka connector sink for AWS kinesis data streams.

Inspired from / Fork of: [AWSLabs Connector](https://github.com/awslabs/kinesis-kafka-connector)

### To run locally

1. From the project root: `mvn package`
2. Configure `config/worker.properties` for kafka connect worker
3. Configure `config/kafka-kinesis-streams-connector.properties` for connector specific properties
4. The full list of properties is [here](src/main/java/com/xing/connectors/KinesisConfig.java)
5. Add the path to the connector jar in the `worker.properties`:
    ```properties
    plugin.path=<path-to-project>/kafka-kinesis-connector/target/kafka-kinesis-connector-0.0.1-SNAPSHOT.jar
    ```
6. Run it with `connect-standalone` like so:
    ```bash
    connect-standalone.sh $PWD/config/worker.properties $PWD/config/kafka-kinesis-streams-connector.properties
    ```
7. If running with saml, it needs the AWS env vars for credentials:
    ```bash
   saml2aws exec -- connect-standalone.sh $PWD/config/worker.properties $PWD/config/kafka-kinesis-streams-connector.properties
    ```
