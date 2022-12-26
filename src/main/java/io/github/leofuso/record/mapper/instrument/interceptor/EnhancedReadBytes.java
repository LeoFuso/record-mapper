package io.github.leofuso.record.mapper.instrument.interceptor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.avro.AvroTypeException;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.ResolvingDecoder;
import org.apache.avro.io.parsing.Parser;
import org.apache.avro.io.parsing.Symbol;
import org.apache.avro.util.Utf8;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

public final class EnhancedReadBytes extends AbstractInterceptor<ByteBuffer> {

    public EnhancedReadBytes(final ResolvingDecoder self, final Object[] ignored) {
        super(self, () -> self.readBytes(null));
    }

    private Object readBytes() throws IOException {
        final Parser parser = parser(Parser.class);
        final JsonDecoder in = in();

        final Symbol actual = parser.advance(Symbol.BYTES);
        if (actual == Symbol.STRING) {
            final Utf8 value = in.readString(null);
            return ByteBuffer.wrap(value.getBytes(), 0, value.getByteLength());
        }
        return doReadBytes();
    }

    private Object doReadBytes() throws IOException {
        final JsonParser in = parser(JsonParser.class);

        advance(Symbol.BYTES);
        final JsonToken currentToken = in.getCurrentToken();

        return switch (currentToken) {
            case VALUE_STRING -> {
                final String text = in.getText();
                byte[] result = text.getBytes(StandardCharsets.ISO_8859_1);
                final ByteBuffer value = ByteBuffer.wrap(result);
                in.nextToken();
                yield value;
            }
            case VALUE_NUMBER_FLOAT -> {
                final double value = in.getDoubleValue();
                in.nextToken();
                yield value;
            }
            case VALUE_NUMBER_INT -> {
                final int value = in.getIntValue();
                in.nextToken();
                yield value;
            }
            case VALUE_NULL -> {
                in.nextToken();
                yield null;
            }
            case END_OBJECT -> null;
            default -> throw new AvroTypeException("Expected [byte] or [number]. Got " + currentToken);
        };
    }

    @Override
    public Object intercept(final Object[] ignored) throws Throwable {
        return readBytes();
    }
}
