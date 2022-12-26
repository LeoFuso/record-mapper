package io.github.leofuso.record.mapper.instrument.interceptor.accessors;

import org.apache.avro.io.Decoder;

public interface DecoderAccessor {

    <T extends Decoder> T accessDecoder();

}
