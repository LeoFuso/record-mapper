package io.github.leofuso.kafka.json2avro.instrument.interceptor.accessors;

import org.apache.avro.io.parsing.Symbol;

public interface ParsingAdvancer {

    void doAdvance(Symbol symbol);

}
