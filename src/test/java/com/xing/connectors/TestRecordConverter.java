package com.xing.connectors;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("conversion")
public class TestRecordConverter {

  @Test
  @DisplayName("Int8 Test")
  public void convertInt8Test() {
    Schema schema = SchemaBuilder.int8();
    ByteBuffer left = RecordConverter.parseValue(schema, (byte) 42);
    ByteBuffer right = ByteBuffer.allocate(1).put((byte) 42);
    assertEquals(left, right);
  }

  @Test
  @DisplayName("Short Test")
  public void convertInt16Test() {
    Schema schema = SchemaBuilder.int16();
    ByteBuffer left = RecordConverter.parseValue(schema, (short) 42);
    ByteBuffer right = ByteBuffer.allocate(2).putShort((short) 42);
    assertEquals(left, right);
  }

  @Test
  @DisplayName("Int Test")
  public void convertInt32Test() {
    Schema schema = SchemaBuilder.int32();
    ByteBuffer left = RecordConverter.parseValue(schema, 42);
    ByteBuffer right = ByteBuffer.allocate(4).putInt(42);
    assertEquals(left, right);
  }

  @Test
  @DisplayName("Long Test")
  public void convertInt64Test() {
    Schema schema = SchemaBuilder.int64();
    ByteBuffer left = RecordConverter.parseValue(schema, (long) 42);
    ByteBuffer right = ByteBuffer.allocate(8).putLong(42L);
    assertEquals(left, right);
  }

  @Test
  @DisplayName("Float Test")
  public void convertFloat32Test() {
    Schema schema = SchemaBuilder.float32();
    ByteBuffer left = RecordConverter.parseValue(schema, (float) 42.0);
    ByteBuffer right = ByteBuffer.allocate(4).putFloat((float) 42.0);
    assertEquals(left, right);
  }

  @Test
  @DisplayName("Double Test")
  public void convertFloat64Test() {
    Schema schema = SchemaBuilder.float64();
    ByteBuffer left = RecordConverter.parseValue(schema, (double) 42.0);
    ByteBuffer right = ByteBuffer.allocate(8).putDouble(42.0D);
    assertEquals(left, right);
  }

  @Test
  @DisplayName("Boolean Test")
  public void convertBooleanTest() {
    Schema schema = SchemaBuilder.bool();
    ByteBuffer left = RecordConverter.parseValue(schema, true);
    ByteBuffer right = ByteBuffer.allocate(1).put((byte) 1);
    assertEquals(left, right);
  }

  @Test
  @DisplayName("String Test")
  public void convertStringTest() {
    Schema schema = SchemaBuilder.string();
    ByteBuffer left = RecordConverter.parseValue(schema, "Testing");
    ByteBuffer right = ByteBuffer.wrap("Testing".getBytes(StandardCharsets.UTF_8));
    assertEquals(left, right);
  }

  @Test
  @DisplayName("Array Test")
  public void convertArrayTest() {
    Schema schema = SchemaBuilder.int8();
    Schema arraySchema = SchemaBuilder.array(schema);

    Object[] arrayInt8 = {(byte) 42, (byte) 43, (byte) 44};
    ByteBuffer left = RecordConverter.parseValue(arraySchema, arrayInt8);
    ByteBuffer right = ByteBuffer.allocate(3)
        .put((byte) 42)
        .put((byte) 43)
        .put((byte) 44);
    assertEquals(left, right);
  }

  @Test
  @DisplayName("Bytes Test")
  public void convertByteTest() {
    Schema schema = SchemaBuilder.bytes();
    byte[] value = "KinesisSink".getBytes(StandardCharsets.UTF_8);
    ByteBuffer left = RecordConverter.parseValue(schema, value);
    ByteBuffer right = ByteBuffer.wrap("KinesisSink".getBytes(StandardCharsets.UTF_8));
    assertEquals(left, right);
  }

  @Test
  @DisplayName("Struct Test")
  public void convertStructTest() {
    String expected = "{\"education_id\":\"201\",\"profile_id\":\"2\",\"school_name\":\"SRM\","
        + "\"degree\":\"EEE\",\"dt\":\"2022-12-20\",\"_hoodie_is_deleted\":\"false\"}";

    Schema schema = SchemaBuilder.struct()
        .field("education_id", Schema.INT32_SCHEMA)
        .field("profile_id", Schema.INT32_SCHEMA)
        .field("school_name", Schema.STRING_SCHEMA)
        .field("degree", Schema.STRING_SCHEMA)
        .field("dt", Schema.STRING_SCHEMA)
        .field("_hoodie_is_deleted", Schema.BOOLEAN_SCHEMA)
        .build();

    Struct value = new Struct(schema);
    value.put("education_id", 201);
    value.put("profile_id", 2);
    value.put("school_name", "SRM");
    value.put("degree", "EEE");
    value.put("dt", "2022-12-20");
    value.put("_hoodie_is_deleted", false);

    ByteBuffer record = RecordConverter.parseValue(schema, value);
    String recordString = String.valueOf(StandardCharsets.UTF_8.decode(record));
    assertEquals(recordString, expected);
  }
}

