package io.github.leofuso.kafka.json2avro.instrument.interceptor.accessors;

import org.apache.avro.io.Decoder;

public interface DecoderAccessor {

    <T extends Decoder> T accessDecoder();

}
