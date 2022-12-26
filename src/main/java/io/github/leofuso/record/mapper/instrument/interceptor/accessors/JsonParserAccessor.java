package io.github.leofuso.record.mapper.instrument.interceptor.accessors;

import com.fasterxml.jackson.core.JsonParser;

public interface JsonParserAccessor {

    <T extends JsonParser> T accessJsonParser();

}
