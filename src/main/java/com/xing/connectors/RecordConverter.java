package com.xing.connectors;

import com.amazonaws.services.kinesis.model.Record;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.UnsupportedEncodingException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecordConverter {

  private static final Logger logger = LoggerFactory.getLogger(RecordConverter.class);

  /**
   * Parse Kafka Values and converts them into Kinesis records
   *
   * @param schema Schema of passed message
   * @param value  Value of the message
   * @return ByteBuffer of record as per the schema
   */
  public static ByteBuffer parseValue(Schema schema, Object value) {
    if (value == null) {
      return null;
    }

    Schema.Type t = schema.type();
    switch (t) {
      case INT8:
        ByteBuffer smallIntBuffer = ByteBuffer.allocate(1);
        smallIntBuffer.put((Byte) value);
        return smallIntBuffer;

      case INT16:
        ByteBuffer shortBuffer = ByteBuffer.allocate(2);
        shortBuffer.putShort((Short) value);
        return shortBuffer;

      case INT32:
        ByteBuffer intBuffer = ByteBuffer.allocate(4);
        intBuffer.putInt((Integer) value);
        return intBuffer;

      case INT64:
        ByteBuffer longBuffer = ByteBuffer.allocate(8);
        longBuffer.putLong((Long) value);
        return longBuffer;

      case FLOAT32:
        ByteBuffer floatBuffer = ByteBuffer.allocate(4);
        floatBuffer.putFloat((Float) value);
        return floatBuffer;

      case FLOAT64:
        ByteBuffer doubleBuffer = ByteBuffer.allocate(8);
        doubleBuffer.putDouble((Double) value);
        return doubleBuffer;

      case BOOLEAN:
        ByteBuffer boolBuffer = ByteBuffer.allocate(1);
        boolBuffer.put((byte) ((Boolean) value ? 1 : 0));
        return boolBuffer;

      case STRING:
        try {
          return ByteBuffer.wrap(((String) value).getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
          logger.error("Encoding error (UTF-8): " + e.getLocalizedMessage());
        } catch (Exception e) {
          logger.error("Unexpected error: " + e.getLocalizedMessage());
          throw e;
        }

      case ARRAY:
        Schema s = schema.valueSchema();
        if (s.type() == Type.MAP || s.type() == Type.STRUCT) {
          throw new DataException("Invalid schema type.");
        }
        Object[] objs = (Object[]) value;
        ByteBuffer[] buf = new ByteBuffer[objs.length];

        int numByteBuffer = 0;
        // Iterate and fill with each element in the array
        // Type stays the same
        for (Object obj : objs) {
          buf[numByteBuffer++] = parseValue(s, obj);
        }
        // Allocate a byte buffer ->
        // Stream[ByteBuffer] -> Add the total buffer size of all
        ByteBuffer result = ByteBuffer.allocate(
            Arrays.stream(buf).mapToInt(Buffer::remaining).sum());
        Arrays.stream(buf).forEach(bb -> result.put(bb.duplicate()));
        return result;

      case BYTES:
        if (value instanceof byte[]) {
          return ByteBuffer.wrap((byte[]) value);
        } else {
          if (value instanceof ByteBuffer) {
            return (ByteBuffer) value;
          }
        }

      case MAP:
        return ByteBuffer.wrap(new byte[0]);

      case STRUCT:
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode record = mapper.createObjectNode();
        schema.fields().forEach(
            field -> {
              record.put(field.name(), ((Struct) value).get(field).toString());
            }
        );
        try {
          String json = mapper.writeValueAsString(record);
          return ByteBuffer.wrap(json.getBytes(StandardCharsets.UTF_8));
        } catch (JsonProcessingException e) {
          throw new DataException("Failed with: " + e.getLocalizedMessage());
        }

    }

    return null;
  }

  /**
   * Converts Kafka message to Kinesis Record
   *
   * @param record Kafka message
   * @return Kinesis record
   */
  public static Record createRecord(SinkRecord record) {
    return new Record().withData(parseValue(record.valueSchema(), record.value()));
  }

}
