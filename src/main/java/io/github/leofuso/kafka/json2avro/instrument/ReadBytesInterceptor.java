package io.github.leofuso.kafka.json2avro.instrument;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.avro.AvroTypeException;
import org.apache.avro.io.ResolvingDecoder;
import org.apache.avro.io.parsing.Symbol;
import org.apache.avro.util.Utf8;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

public final class ReadBytesInterceptor extends AbstractInterceptor<ByteBuffer> {

    static Object intercept(final ResolvingDecoder self, final Object[] arguments) {
        return new ReadBytesInterceptor(self, arguments)
                .apply(arguments);
    }

    ReadBytesInterceptor(final ResolvingDecoder self, final Object[] arguments) {
        super(self, () -> self.readBytes((ByteBuffer) arguments[0]));
    }

    private Object readBytes(final ByteBuffer ignored) throws IOException {
        final Symbol actual = advanceBoth(Symbol.BYTES);
        if (actual == Symbol.STRING) {
            final Utf8 stringValue = getDecoder().readString(null);
            return ByteBuffer.wrap(stringValue.getBytes(), 0, stringValue.getByteLength());
        }
        return doReadBytes(ignored);
    }

    private Object doReadBytes(final ByteBuffer ignored) throws IOException {
        final JsonParser in = getParser();
        final JsonToken currentToken = in.getCurrentToken();

        return switch (currentToken) {
            case VALUE_STRING -> {
                final String text = in.getText();
                byte[] result = text.getBytes(StandardCharsets.ISO_8859_1);
                in.nextToken();
                yield ByteBuffer.wrap(result);
            }
            case VALUE_NUMBER_INT, VALUE_NUMBER_FLOAT -> {
                final double value = in.getDoubleValue();
                in.nextToken();
                yield value;
            }
            default -> throw new AvroTypeException("Expected [byte] or [number]. Got " + currentToken);
        };
    }

    @Override
    public Object intercept(final Object[] arguments) throws Throwable {
        return readBytes((ByteBuffer) arguments[0]);
    }
}
