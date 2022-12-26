package io.github.leofuso.kafka.json2avro.internal;

import org.apache.avro.Conversion;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import io.github.leofuso.kafka.json2avro.RecordReaderFactory;
import io.github.leofuso.kafka.json2avro.RecordWriterFactory;

public class DefaultRecordReaderWriterFactory implements RecordReaderFactory, RecordWriterFactory {

    private final GenericData data;

    DefaultRecordReaderWriterFactory(final Conversion<?>... additional) {
        data = GenericData.get();
        for (final Conversion<?> conversion : additional) {
            data.addLogicalTypeConversion(conversion);
        }
    }

    @Override
    public <T extends SpecificRecord> SpecificDatumReader<T> produceReader(final Class<T> type) {
        return new SpecificDatumReader<>(type);
    }

    @Override
    public GenericDatumReader<GenericData.Record> produceReader(final Schema schema) {
        return new GenericDatumReader<>(schema, schema, data);
    }

    @Override
    public <T extends SpecificRecord> SpecificDatumWriter<T> produceWriter(final Class<T> type) {
        return new SpecificDatumWriter<>(type);
    }

    @Override
    public GenericDatumWriter<GenericData.Record> produceWriter(final Schema schema) {
        return new GenericDatumWriter<>(schema, data);
    }
}
