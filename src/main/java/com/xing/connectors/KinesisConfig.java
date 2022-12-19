package com.xing.connectors;

import java.util.Properties;

/**
 * Configuration for Kinesis Worker.
 */
public class KinesisConfig {

  private Properties props;

  public static final String REGION = "region";
  public static final String STREAM_NAME = "streamName";
  public static final String MAX_BUFFERED_TIME = "maxBufferedTime";
  public static final String MAX_CONNECTIONS = "maxConnections";
  public static final String RATE_LIMIT = "rateLimit";
  public static final String RECORD_TTL = "ttl";
  public static final String METRICS_LEVEL = "metricsLevel";
  public static final String METRICS_GRANULARITY = "metricsGranularity";
  public static final String METRICS_NAMESPACE = "metricsNamespace";
  public static final String AGGREGATION_ENABLED = "aggregation";
  public static final String USE_PARTITION_AS_HASH_KEY = "usePartitionAsHashKey";
  public static final String FLUSH_SYNC = "flushSync";
  public static final String
      SINGLE_KINESIS_PRODUCER_PER_PARTITION = "singleKinesisProducerPerPartition";
  public static final String PAUSE_CONSUMPTION = "pauseConsumption";
  public static final String OUTSTANDING_RECORDS_THRESHOLD = "outstandingRecordsThreshold";
  public static final String SLEEP_PERIOD = "sleepPeriod";
  public static final String SLEEP_CYCLES = "sleepCycles";
  // AWS Role args
  public static final String ROLE_ARN = "roleARN";
  public static final String ROLE_SESSION_NAME = "roleSessionName";
  public static final String ROLE_EXTERNAL_ID = "roleExternalID";
  public static final String ROLE_DURATION_SECONDS = "roleDurationSeconds";
  public static final String KINESIS_ENDPOINT = "kinesisEndpoint";


  public KinesisConfig() {
    this.props = new Properties();
  }

  /**
   * Builder for config.
   */
  public static class Builder {

    private final KinesisConfig config = new KinesisConfig();

    public KinesisConfig.Builder fromProperties(Properties props) {
      this.config.props.putAll(props);
      return this;
    }

    public Builder withRegion(String region) {
      config.props.setProperty(REGION, region);
      return this;
    }

    public Builder withStreamName(String streamName) {
      config.props.setProperty(STREAM_NAME, streamName);
      return this;
    }

    public Builder withMaxBufferedTime(String maxBufferedTime) {
      config.props.setProperty(MAX_BUFFERED_TIME, maxBufferedTime);
      return this;
    }

    public Builder withMaxConnections(String maxConnections) {
      config.props.setProperty(MAX_CONNECTIONS, maxConnections);
      return this;
    }

    public Builder withRateLimit(String rateLimit) {
      config.props.setProperty(RATE_LIMIT, rateLimit);
      return this;
    }

    public Builder withTtl(String ttl) {
      config.props.setProperty(RECORD_TTL, ttl);
      return this;
    }

    public Builder withMetricsLevel(String metricsLevel) {
      config.props.setProperty(METRICS_LEVEL, metricsLevel);
      return this;
    }

    public Builder withMetricsGranularity(String metricsGranularity) {
      config.props.setProperty(METRICS_GRANULARITY, metricsGranularity);
      return this;
    }

    public Builder withMetricsNamespace(String metricsNamespace) {
      config.props.setProperty(METRICS_NAMESPACE, metricsNamespace);
      return this;
    }

    public Builder withAggregation(String aggregation) {
      config.props.setProperty(AGGREGATION_ENABLED, aggregation);
      return this;
    }

    public Builder withUsePartitionAsHashKey(String usePartitionAsHashKey) {
      config.props.setProperty(USE_PARTITION_AS_HASH_KEY, usePartitionAsHashKey);
      return this;
    }

    public Builder withFlushSync(String flushSync) {
      config.props.setProperty(FLUSH_SYNC, flushSync);
      return this;
    }

    public Builder withSingleKinesisProducerPerPartition(String singleKinesisProducerPerPartition) {
      config.props.setProperty(SINGLE_KINESIS_PRODUCER_PER_PARTITION,
          singleKinesisProducerPerPartition);
      return this;
    }

    public Builder withPauseConsumption(String pauseConsumption) {
      config.props.setProperty(PAUSE_CONSUMPTION, pauseConsumption);
      return this;
    }

    public Builder withOutstandingRecordsThreshold(String outstandingRecordsThreshold) {
      config.props.setProperty(OUTSTANDING_RECORDS_THRESHOLD, outstandingRecordsThreshold);
      return this;
    }

    public Builder withSleepPeriod(String sleepPeriod) {
      config.props.setProperty(SLEEP_PERIOD, sleepPeriod);
      return this;
    }

    public Builder withSleepCycles(String sleepCycles) {
      config.props.setProperty(SLEEP_CYCLES, sleepCycles);
      return this;
    }

    public Builder withRoleArn(String roleArn) {
      config.props.setProperty(ROLE_ARN, roleArn);
      return this;
    }

    public Builder withRoleSessionName(String roleSessionName) {
      config.props.setProperty(ROLE_SESSION_NAME, roleSessionName);
      return this;
    }

    public Builder withRoleExternalId(String roleExternalId) {
      config.props.setProperty(ROLE_EXTERNAL_ID, roleExternalId);
      return this;
    }

    public Builder withRoleDurationSeconds(String roleDurationSeconds) {
      config.props.setProperty(ROLE_DURATION_SECONDS, roleDurationSeconds);
      return this;
    }

    public Builder withKinesisEndpoint(String kinesisEndpoint) {
      config.props.setProperty(KINESIS_ENDPOINT, kinesisEndpoint);
      return this;
    }
  }
}
