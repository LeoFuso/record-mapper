package io.github.leofuso.kafka.json2avro;

import org.junit.jupiter.api.Test;

public class ByteBuddyTest {

    @Test
    void stuff() {
        final JsonMapperFactory factory = JsonMapperFactory.get();
        final JsonMapper mapper = factory.produce();

        mapper.asGenericDataRecord((byte[]) null, null);
    }
}
