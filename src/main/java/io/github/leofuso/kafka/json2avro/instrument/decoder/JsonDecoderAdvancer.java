package io.github.leofuso.kafka.json2avro.instrument.decoder;

import org.apache.avro.io.parsing.Symbol;

public interface JsonDecoderAdvancer {

    void doAdvance(Symbol symbol);

}
