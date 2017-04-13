package com.amazonaws.services.kinesis.stormspout;

import com.amazonaws.services.kinesis.model.Record;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.nio.ByteBuffer;
import java.util.Date;

public class RecordSerializer extends Serializer<Record> {
  @Override
  public void write(Kryo kryo, Output output, Record record) {
    output.writeString(record.getPartitionKey());
    output.writeString(record.getSequenceNumber());
    output.writeBoolean(record.getApproximateArrivalTimestamp() != null);
    if (record.getApproximateArrivalTimestamp() != null) {
      output.writeLong(record.getApproximateArrivalTimestamp().getTime());
    }

    byte[] bytes = SerializationHelper.copyData(record.getData());
    output.writeInt(bytes.length);
    output.writeBytes(bytes);
  }

  @Override
  public Record read(Kryo kryo, Input input, Class<Record> aClass) {
    Record record = new Record();

    record.setPartitionKey(input.readString());
    record.setSequenceNumber(input.readString());

    if (input.readBoolean()) {
      record.setApproximateArrivalTimestamp(new Date(input.readLong()));
    }

    int length = input.readInt();
    byte[] bytes = input.readBytes(length);
    record.setData(ByteBuffer.wrap(bytes));

    return record;
  }
}
