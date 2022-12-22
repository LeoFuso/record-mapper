package io.github.leofuso.kafka.json2avro.instrument.interceptor;

import java.io.IOException;

import org.apache.avro.AvroTypeException;
import org.apache.avro.io.ResolvingDecoder;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

public final class ReadIntInterceptor extends AbstractInterceptor<Integer> {

    public static Object intercept(final ResolvingDecoder self) {
        return new ReadIntInterceptor(self)
                .apply(null);
    }

    private ReadIntInterceptor(final ResolvingDecoder self) {
        super(self, self::readInt);
    }

    private Object readInt() throws IOException {
        final JsonParser in = getParser();
        final JsonToken currentToken = in.nextValue();

        return switch (currentToken) {
            case VALUE_STRING -> {
                final String value = in.getText();
                in.nextToken();
                yield value;
            }
            case VALUE_NUMBER_INT, VALUE_NUMBER_FLOAT -> {
                final int value = in.getIntValue();
                in.nextToken();
                yield value;
            }
            case END_OBJECT, VALUE_NULL -> null;
            default -> throw new AvroTypeException("Expected [CharSequence] or [number]. Got " + currentToken);
        };
    }

    @Override
    public Object intercept(final Object[] ignored) throws Throwable {
        return readInt();
    }
}
