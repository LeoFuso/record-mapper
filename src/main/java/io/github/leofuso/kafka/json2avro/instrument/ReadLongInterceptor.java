package io.github.leofuso.kafka.json2avro.instrument;

import java.io.IOException;

import org.apache.avro.AvroTypeException;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.ResolvingDecoder;
import org.apache.avro.io.parsing.Symbol;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

public final class ReadLongInterceptor extends AbstractInterceptor<Long> {

    static Object intercept(final ResolvingDecoder self) {
        return new ReadLongInterceptor(self)
                .apply(null);
    }

    ReadLongInterceptor(final ResolvingDecoder self) {
        super(self, self::readLong);
    }

    private Object readLong() throws IOException {
        final JsonDecoder in = getDecoder();
        final Symbol actual = advanceBoth(Symbol.LONG);
        if (actual == Symbol.INT) {
            return in.readInt();
        } else if (actual == Symbol.DOUBLE) {
            return (long) in.readDouble();
        } else {
            return doReadLong();
        }
    }

    private Object doReadLong() throws IOException {
        final JsonParser in = getParser();
        final JsonToken currentToken = in.getCurrentToken();

        return switch (currentToken) {
            case VALUE_STRING -> {
                final String value = in.getText();
                in.nextToken();
                yield value;
            }
            case VALUE_NUMBER_INT, VALUE_NUMBER_FLOAT -> {
                final long value = in.getLongValue();
                in.nextToken();
                yield value;
            }
            case END_OBJECT -> null;
            default -> throw new AvroTypeException("Expected [CharSequence] or [number]. Got " + currentToken);
        };
    }

    @Override
    public Object intercept(final Object[] ignored) throws Throwable {
        return readLong();
    }
}
