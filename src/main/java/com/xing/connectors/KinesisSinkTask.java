package com.xing.connectors;

import com.amazonaws.services.kinesis.producer.Attempt;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordFailedException;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.amazonaws.util.StringUtils;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KinesisSinkTask extends SinkTask {

  private static final Logger logger = LoggerFactory.getLogger(KinesisSinkTask.class);

  private String streamName;
  private String regionName;
  private String roleARN;
  private String roleExternalID;
  private String roleSessionName;
  private int roleDurationSeconds;
  private String kinesisEndpoint;
  private int maxConnections;
  private int rateLimit;
  private int maxBufferedTime;
  private int ttl;
  private String metricsLevel;
  private String metricsGranularity;
  private String metricsNamespace;
  private boolean aggregation;
  private boolean usePartitionAsHashKey;
  private boolean flushSync;
  private boolean singleKinesisProducerPerPartition;
  private boolean pauseConsumption;
  private int outstandingRecordsThreshold;
  private int sleepPeriod;
  private int sleepCycles;

  private SinkTaskContext sinkTaskContext;
  private Map<String, KinesisProducer> producerMap = new HashMap<>();
  private KinesisProducer kinesisProducer;
  private ConnectException putException;

  // Future callback
  final FutureCallback<UserRecordResult> callback = new FutureCallback<UserRecordResult>() {
    @Override
    public void onSuccess(@Nullable UserRecordResult userRecordResult) {
      if (userRecordResult != null) {
        logger.info("Future successful: \nAttempts: " + userRecordResult.getAttempts()
            + "\nSequenceNumber: " + userRecordResult.getSequenceNumber() +
            "\nShardID: " + userRecordResult.getShardId() + "\nSuccess: "
            + userRecordResult.isSuccessful());
      }
    }

    @Override
    public void onFailure(Throwable throwable) {
      if (throwable instanceof UserRecordFailedException) {
        Attempt last = Iterables.getLast(((UserRecordFailedException) throwable).getResult()
            .getAttempts());
        putException = new RetriableException(
            "Kinesis Producer failed - " + last.getErrorCode() + "\t" + last.getErrorMessage());
        return;
      }
      putException = new RetriableException("Exception during Kinesis putRecord: ", throwable);
    }
  };

  private void hasPutExceptionBefore() {
    if (putException != null) {
      final ConnectException e = putException;
      putException = null;
      throw e;
    }
  }

  @Override
  public void initialize(SinkTaskContext context) {
    sinkTaskContext = context;
  }

  @Override
  public String version() {
    return null;
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> arg0) {
    hasPutExceptionBefore();

    if (singleKinesisProducerPerPartition) {
      producerMap.values().forEach(producer -> {
        if (flushSync) {
          producer.flushSync();
        } else {
          producer.flush();
        }
      });
    } else {
      if (flushSync) {
        kinesisProducer.flushSync();
      } else {
        kinesisProducer.flush();
      }
    }
  }

  @Override
  public void start(Map<String, String> props) {
    streamName = props.get(KinesisConfig.STREAM_NAME);
    maxConnections = Integer.parseInt(props.get(KinesisConfig.MAX_CONNECTIONS));
    rateLimit = Integer.parseInt(props.get(KinesisConfig.RATE_LIMIT));
    maxBufferedTime = Integer.parseInt(props.get(KinesisConfig.MAX_BUFFERED_TIME));
    ttl = Integer.parseInt(props.get(KinesisConfig.RECORD_TTL));
    regionName = props.get(KinesisConfig.REGION);
    roleARN = props.get(KinesisConfig.ROLE_ARN);
    roleSessionName = props.get(KinesisConfig.ROLE_SESSION_NAME);
    roleDurationSeconds = Integer.parseInt(props.get(KinesisConfig.ROLE_DURATION_SECONDS));
    roleExternalID = props.get(KinesisConfig.ROLE_EXTERNAL_ID);
    kinesisEndpoint = props.get(KinesisConfig.KINESIS_ENDPOINT);
    metricsLevel = props.get(KinesisConfig.METRICS_LEVEL);
    metricsGranularity = props.get(KinesisConfig.METRICS_GRANULARITY);
    metricsNamespace = props.get(KinesisConfig.METRICS_NAMESPACE);
    aggregation = Boolean.parseBoolean(props.get(KinesisConfig.AGGREGATION_ENABLED));
    usePartitionAsHashKey = Boolean.parseBoolean(
        props.get(KinesisConfig.USE_PARTITION_AS_HASH_KEY));
    flushSync = Boolean.parseBoolean(props.get(KinesisConfig.FLUSH_SYNC));
    singleKinesisProducerPerPartition = Boolean.parseBoolean(
        props.get(KinesisConfig.SINGLE_KINESIS_PRODUCER_PER_PARTITION));
    pauseConsumption = Boolean.parseBoolean(props.get(KinesisConfig.PAUSE_CONSUMPTION));
    outstandingRecordsThreshold = Integer.parseInt(
        props.get(KinesisConfig.OUTSTANDING_RECORDS_THRESHOLD));
    sleepPeriod = Integer.parseInt(props.get(KinesisConfig.SLEEP_PERIOD));
    sleepCycles = Integer.parseInt(props.get(KinesisConfig.SLEEP_CYCLES));

    if (!singleKinesisProducerPerPartition) {
      kinesisProducer = getKinesisProducer();
    }
    putException = null;
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    hasPutExceptionBefore();
    validateOutstandingRecords();

    String partitionKey;

    for (SinkRecord record : records) {
      ListenableFuture<UserRecordResult> f;
      // Partitionkey for Kinesis
      if (record.key() != null && !record.key().toString().trim().equals("")) {
        partitionKey = record.key().toString().trim();
      } else {
        partitionKey = Integer.toString(record.kafkaPartition());
      }
      // KinesisProducer Send Func -> Add to Future
      if (singleKinesisProducerPerPartition) {
        f = addUserRecord(producerMap.get(record.kafkaPartition() + "@" + record.topic()),
            streamName, partitionKey, usePartitionAsHashKey, record);
      } else {
        f = addUserRecord(kinesisProducer, streamName, partitionKey, usePartitionAsHashKey, record);
      }

      // Add the callback to the future
      Futures.addCallback(f, callback, MoreExecutors.directExecutor());

    }
  }

  /**
   * Check if data is being sent to Kinesis If not, pause consumption until backlog is cleared.
   * Flag: PauseConsumption is turned on
   *
   * @return
   */
  private boolean validateOutstandingRecords() {
    if (pauseConsumption) {
      if (singleKinesisProducerPerPartition) {
        producerMap.values().forEach(producer -> {
          // Wait for backlog to clear out
          int sleepCount = 0;
          boolean pause = false;
          // Check if outstanding records are within threshold
          while (producer.getOutstandingRecordsCount() > outstandingRecordsThreshold) {
            try {
              sinkTaskContext.pause((TopicPartition[]) sinkTaskContext.assignment().toArray());
              pause = true;
              Thread.sleep(sleepPeriod);
              if (sleepCount++ > sleepCycles) {
                logger.error(
                    "Exceeded max sleep cycles: " + sleepCycles
                        + ", Cannot send records to kinesis as the buffer threshold has been reached");
                sleepCount = 0;
              }
            } catch (InterruptedException e) {
              logger.error("Pause consumption thread interrupted, " + e.getMessage());
              e.printStackTrace();
            }
          }
          // if pause is true, backlog is cleared or < threshold
          if (pause) {
            sinkTaskContext.resume((TopicPartition[]) sinkTaskContext.assignment().toArray());
          }

        });
        return true;
      } // Single producer for all partitions
    } else {
      int sleepCount = 0;
      boolean pause = false;

      while (kinesisProducer.getOutstandingRecordsCount() > outstandingRecordsThreshold) {
        try {
          sinkTaskContext.pause((TopicPartition[]) sinkTaskContext.assignment().toArray());
          pause = true;
          Thread.sleep(sleepPeriod);
          if (sleepCount++ > sleepCycles) {
            logger.error(
                "Exceeded max sleep cycles: " + sleepCycles
                    + ", Cannot send records to kinesis as the buffer threshold has been reached");
            sleepCount = 0;
          }
        } catch (InterruptedException e) {
          logger.error("Pause consumption thread interrupted, " + e.getMessage());
          e.printStackTrace();
        }
      }
      if (pause) {
        sinkTaskContext.resume((TopicPartition[]) sinkTaskContext.assignment().toArray());
      }
      return true;
    }
    return true;
  }

  private ListenableFuture<UserRecordResult> addUserRecord(KinesisProducer kp, String streamName,
      String partitionKey,
      boolean usePartitionAsHashKey, SinkRecord record) {
    // We can use the Kafka Partition as the partition key for Kinesis
    // same partition -> same shard
    // LOG
    logger.info("Received record: " + record.toString());
    logger.info("Received record value: " + record.value().toString());
    logger.info("Received record value schema: " + record.valueSchema().toString());
    logger.info("Received record value schema (object): " + record.valueSchema());
    logger.info(
        "Record parsed as: " + StandardCharsets.UTF_8.decode(
            RecordConverter.parseValue(record.valueSchema(), record.value()))
    );
    // END LOG
    if (usePartitionAsHashKey) {
      return kp.addUserRecord(streamName, partitionKey, Integer.toString(record.kafkaPartition()),
          RecordConverter.parseValue(record.valueSchema(), record.value()));
    } else {
      return kp.addUserRecord(streamName, partitionKey,
          RecordConverter.parseValue(record.valueSchema(), record.value()));
    }
  }

  public void open(Collection<TopicPartition> partitions) {
    if (singleKinesisProducerPerPartition) {
      for (TopicPartition part : partitions) {
        producerMap.put(part.partition() + "@" + part.topic(), getKinesisProducer());
      }
    }
  }

  public void close(Collection<TopicPartition> partitions) {
    if (singleKinesisProducerPerPartition) {
      for (TopicPartition part : partitions) {
        producerMap.get(part.partition() + "@" + part.topic()).destroy();
        producerMap.remove(part.partition() + "@" + part.topic());
      }
    }
  }

  @Override
  public void stop() {
    if (singleKinesisProducerPerPartition) {
      for (KinesisProducer kp : producerMap.values()) {
        kp.flushSync();
        kp.destroy();
      }
    } else {
      kinesisProducer.destroy();
    }
  }

  private KinesisProducer getKinesisProducer() {
    KinesisProducerConfiguration config = new KinesisProducerConfiguration();
    config.setRegion(regionName);
    config.setCredentialsProvider(
        CredentialsUtility.createCredentials(regionName, roleARN, roleExternalID, roleSessionName,
            roleDurationSeconds));
    config.setMaxConnections(maxConnections);
    if (!StringUtils.isNullOrEmpty(kinesisEndpoint)) {
      config.setKinesisEndpoint(kinesisEndpoint);
    }
    config.setRateLimit(rateLimit);
    config.setRecordMaxBufferedTime(maxBufferedTime);
    config.setRecordTtl(ttl);
    config.setMetricsLevel(metricsLevel);
    config.setMetricsGranularity(metricsGranularity);
    config.setMetricsNamespace(metricsNamespace);
    return new KinesisProducer(config);
  }
}
