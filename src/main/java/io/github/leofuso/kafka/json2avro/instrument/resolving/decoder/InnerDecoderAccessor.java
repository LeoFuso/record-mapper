package io.github.leofuso.kafka.json2avro.instrument.resolving.decoder;

import org.apache.avro.io.Decoder;

public interface InnerDecoderAccessor {

    Decoder accessDecoder();

    @SuppressWarnings("unchecked")
    default <T extends Decoder> T accessTypedDecoder() {
        return (T) accessDecoder();
    }

}
