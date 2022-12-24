package io.github.leofuso.kafka.json2avro.internal;

import org.apache.avro.Conversion;
import org.apache.avro.Conversions;
import org.apache.avro.RelaxedDecimalConversion;
import org.apache.avro.Schema;
import org.apache.avro.data.RelaxedTimeConversions;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.RelaxedGenericDatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import io.github.leofuso.kafka.json2avro.DatumFactory;
import io.github.leofuso.kafka.json2avro.instrument.bytecode.LogicalTypeConversionEnhancement;

public class RelaxedDatumFactory implements DatumFactory {

    private final GenericData data;

    RelaxedDatumFactory(Conversion<?>... additional) {

        LogicalTypeConversionEnhancement.enhance();

        data = GenericData.get();
        data.addLogicalTypeConversion(new RelaxedDecimalConversion());
        data.addLogicalTypeConversion(new Conversions.UUIDConversion());
        data.addLogicalTypeConversion(new RelaxedTimeConversions.DateConversion());
        data.addLogicalTypeConversion(new RelaxedTimeConversions.TimeMillisConversion());
        data.addLogicalTypeConversion(new RelaxedTimeConversions.TimeMicrosConversion());
        data.addLogicalTypeConversion(new RelaxedTimeConversions.TimestampMillisConversion());
        data.addLogicalTypeConversion(new RelaxedTimeConversions.TimestampMicrosConversion());
        data.addLogicalTypeConversion(new RelaxedTimeConversions.LocalTimestampMillisConversion());
        data.addLogicalTypeConversion(new RelaxedTimeConversions.LocalTimestampMicrosConversion());

        for (Conversion<?> converter : additional) {
            data.addLogicalTypeConversion(converter);
        }
    }

    @Override
    public <T extends SpecificRecord> SpecificDatumReader<T> createReader(final Class<T> type) {
        return new SpecificDatumReader<>(type);
    }

    @Override
    public <T> GenericDatumReader<T> createReader(final Schema schema) {
        return new RelaxedGenericDatumReader<>(schema, schema, data);
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
