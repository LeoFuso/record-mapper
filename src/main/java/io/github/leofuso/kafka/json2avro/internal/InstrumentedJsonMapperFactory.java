package io.github.leofuso.kafka.json2avro.internal;

import org.apache.avro.Conversion;
import org.apache.avro.Conversions;
import org.apache.avro.RelaxedDecimalConversion;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;

import io.github.leofuso.kafka.json2avro.JsonMapper;
import io.github.leofuso.kafka.json2avro.JsonMapperFactory;
import io.github.leofuso.kafka.json2avro.instrument.ByteBuddySetup;

import com.fasterxml.jackson.databind.ObjectMapper;

public class InstrumentedJsonMapperFactory implements JsonMapperFactory {

    public InstrumentedJsonMapperFactory() {
        new ByteBuddySetup().init();
    }

    @Override
    public JsonMapper produce(Conversion<?>... additional) {

        final GenericData data = new GenericData();
        data.addLogicalTypeConversion(new RelaxedDecimalConversion());
        data.addLogicalTypeConversion(new Conversions.UUIDConversion());
        data.addLogicalTypeConversion(new TimeConversions.DateConversion());
        data.addLogicalTypeConversion(new TimeConversions.TimeMillisConversion());
        data.addLogicalTypeConversion(new TimeConversions.TimeMicrosConversion());
        data.addLogicalTypeConversion(new TimeConversions.TimestampMillisConversion());
        data.addLogicalTypeConversion(new TimeConversions.TimestampMicrosConversion());
        data.addLogicalTypeConversion(new TimeConversions.LocalTimestampMillisConversion());
        data.addLogicalTypeConversion(new TimeConversions.LocalTimestampMicrosConversion());

        for (Conversion<?> converter : additional) {
            data.addLogicalTypeConversion(converter);
        }

        final ObjectMapper mapper = ObjectMapperFactory.getInstance();
        return new DefaultJsonMapper(data, mapper);
    }
}
