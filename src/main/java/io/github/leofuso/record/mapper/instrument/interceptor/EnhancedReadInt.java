package io.github.leofuso.record.mapper.instrument.interceptor;

import java.io.IOException;

import org.apache.avro.AvroTypeException;
import org.apache.avro.io.ResolvingDecoder;
import org.apache.avro.io.parsing.Parser;
import org.apache.avro.io.parsing.Symbol;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

public final class EnhancedReadInt extends AbstractInterceptor<Integer> {

    public EnhancedReadInt(final ResolvingDecoder self, final Object[] ignored) {
        super(self, self::readInt);
    }

    private Object readInt() throws IOException {
        final Parser parser = parser(Parser.class);

        parser.advance(Symbol.INT);
        return doReadInt();
    }

    private Object doReadInt() throws IOException {
        final JsonParser in = parser(JsonParser.class);

        advance(Symbol.INT);
        final JsonToken currentToken = in.getCurrentToken();

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
            case VALUE_NULL -> {
                in.nextToken();
                yield null;
            }
            case END_OBJECT -> null;
            default -> throw new AvroTypeException("Expected [CharSequence] or [number]. Got " + currentToken);
        };
    }

    @Override
    public Object intercept(final Object[] ignored) throws Throwable {
        return readInt();
    }
}
