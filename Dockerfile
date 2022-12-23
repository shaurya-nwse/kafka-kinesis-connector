FROM maven:3.8.6-amazoncorretto-8 as build
LABEL author="Shaurya Rawat <shaurya.rawat@new-work.se"

WORKDIR /app
COPY ./pom.xml pom.xml
COPY ./src src/

RUN mvn clean package

FROM confluentinc/cp-kafka-connect:7.0.1 as runner
WORKDIR /app
COPY --from=build /app/target/kafka-kinesis-connector-0.0.1-SNAPSHOT.jar /app/connectors/kafka-kinesis-connector-0.0.1.jar
#COPY ./config /app/config


ENV WORKER_PROPERTIES = ""
ENV KINESIS_CONNECTOR_PROPERTIES = ""
ENV AWS_ACCESS_KEY_ID = ""
ENV AWS_SECRET_ACCESS_KEY = ""
ENV AWS_SESSION_TOKEN = ""
ENV CONNECT_PLUGIN_PATH = "/app/connectors"

ENTRYPOINT ["connect-standalone"]
CMD ["$WORKER_PROPERTIES", "$KINESIS_CONNECTOR_PROPERTIES"]
#CMD ["/app/config/worker.properties", "/app/config/kafka-kinesis-streams-connector.properties"]
