package io.github.leofuso.kafka.json2avro.internal;

import org.apache.avro.Conversion;
import org.apache.avro.Conversions;
import org.apache.avro.EnhancedDecimalConversion;
import org.apache.avro.Schema;
import org.apache.avro.data.EnhancedTimeConversions;
import org.apache.avro.generic.EnhancedGenericDatumReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import io.github.leofuso.kafka.json2avro.RecordReaderFactory;
import io.github.leofuso.kafka.json2avro.RecordWriterFactory;
import io.github.leofuso.kafka.json2avro.instrument.bytecode.LogicalTypeConversionEnhancement;

public class EnhancedRecordReaderWriterFactory implements RecordReaderFactory, RecordWriterFactory {

    private final GenericData data;

    EnhancedRecordReaderWriterFactory(Conversion<?>... additional) {

        LogicalTypeConversionEnhancement.enhance();

        data = GenericData.get();
        data.addLogicalTypeConversion(new EnhancedDecimalConversion());
        data.addLogicalTypeConversion(new Conversions.UUIDConversion());
        data.addLogicalTypeConversion(new EnhancedTimeConversions.DateConversion());
        data.addLogicalTypeConversion(new EnhancedTimeConversions.TimeMillisConversion());
        data.addLogicalTypeConversion(new EnhancedTimeConversions.TimeMicrosConversion());
        data.addLogicalTypeConversion(new EnhancedTimeConversions.TimestampMillisConversion());
        data.addLogicalTypeConversion(new EnhancedTimeConversions.TimestampMicrosConversion());
        data.addLogicalTypeConversion(new EnhancedTimeConversions.LocalTimestampMillisConversion());
        data.addLogicalTypeConversion(new EnhancedTimeConversions.LocalTimestampMicrosConversion());

        for (Conversion<?> converter : additional) {
            data.addLogicalTypeConversion(converter);
        }
    }

    @Override
    public <T extends SpecificRecord> SpecificDatumReader<T> produceReader(final Class<T> type) {
        return new SpecificDatumReader<>(type);
    }

    @Override
    public GenericDatumReader<GenericData.Record> produceReader(final Schema schema) {
        return new EnhancedGenericDatumReader<>(schema, schema, data);
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
