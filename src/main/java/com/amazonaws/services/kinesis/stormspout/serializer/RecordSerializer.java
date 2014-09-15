package com.amazonaws.services.kinesis.stormspout.serializer;

import com.amazonaws.services.kinesis.model.Record;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.nio.ByteBuffer;

/**
 * author: matt <matt@guindin.com>
 */
public class RecordSerializer extends Serializer<Record> {

    @Override
    public void write (Kryo kryo, Output output, Record record) {
        ByteBuffer bb = record.getData();
        if (!bb.hasArray()) {
            throw new KryoException("Unable to read ByteBuffer of data from Record");
        }
        output.writeInt(bb.array().length);
        output.writeBytes(bb.array());
        output.writeString(record.getSequenceNumber());
        output.writeString(record.getPartitionKey());
        kryo.writeClassAndObject(output, null);
    }

    @Override
    public Record read (Kryo kryo, Input input, Class<Record> type) {
        Record record = new Record();
        int size = input.readInt();
        byte[] bytes = new byte[size];
        int count = 0;
        while (count < size) {
            bytes[count] = input.readByte();
            count++;
        }
        record.withData(ByteBuffer.wrap(bytes));
        record.withSequenceNumber(input.readString());
        record.withPartitionKey(input.readString());
        return record;
    }
}
