package io.github.leofuso.kafka.json2avro.instrument.decoder;

import com.fasterxml.jackson.core.JsonParser;

public interface JsonParserAccessor {

    <T extends JsonParser> T accessJsonParser();

}
