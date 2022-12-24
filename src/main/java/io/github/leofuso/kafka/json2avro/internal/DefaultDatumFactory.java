package io.github.leofuso.kafka.json2avro.internal;

import org.apache.avro.Conversion;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import io.github.leofuso.kafka.json2avro.DatumFactory;

public class DefaultDatumFactory implements DatumFactory {

    private final GenericData data;

    DefaultDatumFactory(final Conversion<?>... additional) {
        data = GenericData.get();
        for (final Conversion<?> conversion : additional) {
            data.addLogicalTypeConversion(conversion);
        }
    }

    @Override
    public <T extends SpecificRecord> SpecificDatumReader<T> createReader(final Class<T> type) {
        return new SpecificDatumReader<>(type);
    }

    @Override
    public <T> GenericDatumReader<T> createReader(final Schema schema) {
        return new GenericDatumReader<>(schema, schema, data);
    }

    @Override
    public <T extends SpecificRecord> SpecificDatumWriter<T> createWriter(final Class<T> type) {
        return new SpecificDatumWriter<>(type);
    }

    @Override
    public <T> GenericDatumWriter<T> createWriter(final Schema schema) {
        return new GenericDatumWriter<>(schema, data);
    }
}
