package io.github.leofuso.kafka.json2avro.internal;

import org.apache.avro.Conversion;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
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
    public <T extends GenericRecord> DatumReader<T> makeReader(final Schema schema, final Class<T> type) {
        return SpecificRecord.class.isAssignableFrom(type)
                ? new SpecificDatumReader<>(schema)
                : new GenericDatumReader<>(schema, schema, data);
    }

    @Override
    public <T extends GenericRecord> DatumWriter<T> makeWriter(final Schema schema, final Class<T> type) {
        return SpecificRecord.class.isAssignableFrom(type)
                ? new SpecificDatumWriter<>(schema)
                : new GenericDatumWriter<>(schema, data);
    }
}
