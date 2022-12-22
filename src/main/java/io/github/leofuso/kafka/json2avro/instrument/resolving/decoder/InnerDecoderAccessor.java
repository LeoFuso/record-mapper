package io.github.leofuso.kafka.json2avro.instrument.resolving.decoder;

import org.apache.avro.io.Decoder;

public interface InnerDecoderAccessor {

    <T extends Decoder> T accessDecoder();

}
