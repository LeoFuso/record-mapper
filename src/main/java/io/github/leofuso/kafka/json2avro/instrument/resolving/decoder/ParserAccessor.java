package io.github.leofuso.kafka.json2avro.instrument.resolving.decoder;

import org.apache.avro.io.parsing.Parser;

public interface ParserAccessor {

    Parser accessParser();

}
