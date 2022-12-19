package com.xing.connectors;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
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


}

