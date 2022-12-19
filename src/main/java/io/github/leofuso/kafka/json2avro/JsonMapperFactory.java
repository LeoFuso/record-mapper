package io.github.leofuso.kafka.json2avro;

import org.apache.avro.Conversion;

import io.github.leofuso.kafka.json2avro.internal.InstrumentedJsonMapperFactory;

public interface JsonMapperFactory {

    static JsonMapperFactory get() {
        return new InstrumentedJsonMapperFactory();
    }

    JsonMapper produce(Conversion<?>... additional);

}
