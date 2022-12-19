package io.github.leofuso.kafka.json2avro.instrument;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.avro.AvroTypeException;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.ResolvingDecoder;
import org.apache.avro.io.parsing.Symbol;
import org.apache.avro.util.Utf8;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

public class ReadByteArrayInterceptor extends AbstractInterceptor {

    protected ReadByteArrayInterceptor(final ResolvingDecoder self, final Method superMethod) {
        super(self, superMethod);
    }

    @Override
    public Object intercept(final Object[] arguments) throws Throwable {
        final JsonDecoder decoder = getJsonDecoder();
        final Symbol actual = getParser().advance(Symbol.BYTES);
        if (actual == Symbol.STRING) {
            final Utf8 stringValue = decoder.readString(null);
            return ByteBuffer.wrap(stringValue.getBytes(), 0, stringValue.getByteLength());
        }

        final JsonParser jsonParser = getJsonParser();
        advanceJsonParser(Symbol.BYTES);
        final JsonToken currentToken = jsonParser.getCurrentToken();
        if (currentToken == JsonToken.VALUE_STRING) {
            final String text = jsonParser.getText();
            byte[] result = text.getBytes(StandardCharsets.ISO_8859_1);
            jsonParser.nextToken();
            return ByteBuffer.wrap(result);
        } else if (currentToken.isNumeric()) {
            return jsonParser.getDoubleValue();
        } else {
            throw new AvroTypeException("Expected [byte]. Got " + currentToken);
        }
    }
}
