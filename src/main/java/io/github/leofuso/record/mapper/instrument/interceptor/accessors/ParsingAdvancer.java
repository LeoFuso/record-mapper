package io.github.leofuso.record.mapper.instrument.interceptor.accessors;

import org.apache.avro.io.parsing.Symbol;

public interface ParsingAdvancer {

    void doAdvance(Symbol symbol);

}
