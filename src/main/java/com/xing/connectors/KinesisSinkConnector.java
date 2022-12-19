package com.xing.connectors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;


public class KinesisSinkConnector extends SinkConnector {

  private String region;
  private String streamName;
  private String roleARN;
  private String roleSessionName;
  private String roleExternalID;
  private String roleDurationSeconds;
  private String kinesisEndpoint;
  private String maxBufferedTime;
  private String maxConnections;
  private String rateLimit;
  private String ttl;
  private String metricsLevel;
  private String metricsGranuality;
  private String metricsNameSpace;
  private String aggregation;
  private String usePartitionAsHashKey;
  private String flushSync;
  private String singleKinesisProducerPerPartition;
  private String pauseConsumption;
  private String outstandingRecordsThreshold;
  private String sleepPeriod;
  private String sleepCycles;

  @Override
  public void start(Map<String, String> props) {
    region = props.get(KinesisConfig.REGION);
    streamName = props.get(KinesisConfig.STREAM_NAME);
    roleARN = props.get(KinesisConfig.ROLE_ARN);
    roleSessionName = props.get(KinesisConfig.ROLE_SESSION_NAME);
    roleExternalID = props.get(KinesisConfig.ROLE_EXTERNAL_ID);
    roleDurationSeconds = props.get(KinesisConfig.ROLE_DURATION_SECONDS);
    kinesisEndpoint = props.get(KinesisConfig.KINESIS_ENDPOINT);
    maxBufferedTime = props.get(KinesisConfig.MAX_BUFFERED_TIME);
    maxConnections = props.get(KinesisConfig.MAX_CONNECTIONS);
    rateLimit = props.get(KinesisConfig.RATE_LIMIT);
    ttl = props.get(KinesisConfig.RECORD_TTL);
    metricsLevel = props.get(KinesisConfig.METRICS_LEVEL);
    metricsGranuality = props.get(KinesisConfig.METRICS_GRANULARITY);
    metricsNameSpace = props.get(KinesisConfig.METRICS_NAMESPACE);
    aggregation = props.get(KinesisConfig.AGGREGATION_ENABLED);
    usePartitionAsHashKey = props.get(KinesisConfig.USE_PARTITION_AS_HASH_KEY);
    flushSync = props.get(KinesisConfig.FLUSH_SYNC);
    singleKinesisProducerPerPartition = props.get(
        KinesisConfig.SINGLE_KINESIS_PRODUCER_PER_PARTITION);
    pauseConsumption = props.get(KinesisConfig.PAUSE_CONSUMPTION);
    outstandingRecordsThreshold = props.get(KinesisConfig.OUTSTANDING_RECORDS_THRESHOLD);
    sleepPeriod = props.get(KinesisConfig.SLEEP_PERIOD);
    sleepCycles = props.get(KinesisConfig.SLEEP_CYCLES);
  }

  @Override
  public Class<? extends Task> taskClass() {
    return KinesisSinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    ArrayList<Map<String, String>> configs = new ArrayList<>();
    for (int i = 0; i < maxTasks; i++) {
      Map<String, String> config = new HashMap<>();
      if (streamName != null) {
        config.put(KinesisConfig.STREAM_NAME, streamName);
      }
      if (region != null) {
        config.put(KinesisConfig.REGION, region);
      }
      if (roleARN != null) {
        config.put(KinesisConfig.ROLE_ARN, roleARN);
      }

      if (roleSessionName != null) {
        config.put(KinesisConfig.ROLE_SESSION_NAME, roleSessionName);
      }

      if (roleExternalID != null) {
        config.put(KinesisConfig.ROLE_EXTERNAL_ID, roleExternalID);
      }

      if (roleDurationSeconds != null) {
        config.put(KinesisConfig.ROLE_DURATION_SECONDS, roleDurationSeconds);
      } else {
        config.put(KinesisConfig.ROLE_DURATION_SECONDS, "3600");
      }

      if (kinesisEndpoint != null) {
        config.put(KinesisConfig.KINESIS_ENDPOINT, kinesisEndpoint);
      }

      if (maxBufferedTime != null) {
        config.put(KinesisConfig.MAX_BUFFERED_TIME, maxBufferedTime);
      } else
      // default value of 15000 ms
      {
        config.put(KinesisConfig.MAX_BUFFERED_TIME, "15000");
      }

      if (maxConnections != null) {
        config.put(KinesisConfig.MAX_CONNECTIONS, maxConnections);
      } else {
        config.put(KinesisConfig.MAX_CONNECTIONS, "24");
      }

      if (rateLimit != null) {
        config.put(KinesisConfig.RATE_LIMIT, rateLimit);
      } else {
        config.put(KinesisConfig.RATE_LIMIT, "100");
      }

      if (ttl != null) {
        config.put(KinesisConfig.RECORD_TTL, ttl);
      } else {
        config.put(KinesisConfig.RECORD_TTL, "60000");
      }

      if (metricsLevel != null) {
        config.put(KinesisConfig.METRICS_LEVEL, metricsLevel);
      } else {
        config.put(KinesisConfig.METRICS_LEVEL, "none");
      }

      if (metricsGranuality != null) {
        config.put(KinesisConfig.METRICS_GRANULARITY, metricsGranuality);
      } else {
        config.put(KinesisConfig.METRICS_GRANULARITY, "global");
      }

      if (metricsNameSpace != null) {
        config.put(KinesisConfig.METRICS_NAMESPACE, metricsNameSpace);
      } else {
        config.put(KinesisConfig.METRICS_NAMESPACE, "KinesisProducer");
      }

      if (aggregation != null) {
        config.put(KinesisConfig.AGGREGATION_ENABLED, aggregation);
      } else {
        config.put(KinesisConfig.AGGREGATION_ENABLED, "false");
      }

      if (usePartitionAsHashKey != null) {
        config.put(KinesisConfig.USE_PARTITION_AS_HASH_KEY, usePartitionAsHashKey);
      } else {
        config.put(KinesisConfig.USE_PARTITION_AS_HASH_KEY, "false");
      }

      if (flushSync != null) {
        config.put(KinesisConfig.FLUSH_SYNC, flushSync);
      } else {
        config.put(KinesisConfig.FLUSH_SYNC, "true");
      }

      if (singleKinesisProducerPerPartition != null) {
        config.put(KinesisConfig.SINGLE_KINESIS_PRODUCER_PER_PARTITION,
            singleKinesisProducerPerPartition);
      } else {
        config.put(KinesisConfig.SINGLE_KINESIS_PRODUCER_PER_PARTITION, "false");
      }

      if (pauseConsumption != null) {
        config.put(KinesisConfig.PAUSE_CONSUMPTION, pauseConsumption);
      } else {
        config.put(KinesisConfig.PAUSE_CONSUMPTION, "true");
      }

      if (outstandingRecordsThreshold != null) {
        config.put(KinesisConfig.OUTSTANDING_RECORDS_THRESHOLD, outstandingRecordsThreshold);
      } else {
        config.put(KinesisConfig.OUTSTANDING_RECORDS_THRESHOLD, "500000");
      }

      if (sleepPeriod != null) {
        config.put(KinesisConfig.SLEEP_PERIOD, sleepPeriod);
      } else {
        config.put(KinesisConfig.SLEEP_PERIOD, "1000");
      }

      if (sleepCycles != null) {
        config.put(KinesisConfig.SLEEP_CYCLES, sleepCycles);
      } else {
        config.put(KinesisConfig.SLEEP_CYCLES, "10");
      }

      configs.add(config);

    }
    return configs;
  }

  @Override
  public void stop() {
    // TODO: Implement this
  }

  @Override
  public ConfigDef config() {
    return new ConfigDef();
  }

  @Override
  public String version() {
    return AppInfoParser.getVersion();
  }
}
