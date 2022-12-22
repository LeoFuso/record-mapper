package io.github.leofuso.kafka.json2avro.internal;

import org.apache.avro.Conversion;
import org.apache.avro.Conversions;
import org.apache.avro.RelaxedDecimalConversion;
import org.apache.avro.data.RelaxedTimeConversions;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;

import io.github.leofuso.kafka.json2avro.JsonMapper;
import io.github.leofuso.kafka.json2avro.JsonMapperFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

public class InstrumentedJsonMapperFactory implements JsonMapperFactory {

    @Override
    public JsonMapper produce(Conversion<?>... additional) {

        final GenericData data = new GenericData();
        data.addLogicalTypeConversion(new RelaxedDecimalConversion());
        data.addLogicalTypeConversion(new Conversions.UUIDConversion());
        data.addLogicalTypeConversion(new RelaxedTimeConversions.DateConversion());
        data.addLogicalTypeConversion(new RelaxedTimeConversions.TimeMillisConversion());
        data.addLogicalTypeConversion(new RelaxedTimeConversions.TimeMicrosConversion());
        data.addLogicalTypeConversion(new RelaxedTimeConversions.TimestampMillisConversion());
        data.addLogicalTypeConversion(new RelaxedTimeConversions.TimestampMicrosConversion());
        data.addLogicalTypeConversion(new TimeConversions.LocalTimestampMillisConversion());
        data.addLogicalTypeConversion(new TimeConversions.LocalTimestampMicrosConversion());

        for (Conversion<?> converter : additional) {
            data.addLogicalTypeConversion(converter);
        }

        final ObjectMapper mapper = ObjectMapperFactory.getInstance();
        return new DefaultJsonMapper(data, mapper);
    }
}
