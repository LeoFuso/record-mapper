package io.github.leofuso.kafka.json2avro.instrument;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.ResolvingDecoder;
import org.apache.avro.io.parsing.Parser;
import org.apache.avro.io.parsing.Symbol;

import io.github.leofuso.kafka.json2avro.instrument.resolving.decoder.InnerDecoderAccessor;
import io.github.leofuso.kafka.json2avro.instrument.resolving.decoder.ParserAccessor;

import net.bytebuddy.implementation.bind.annotation.AllArguments;
import net.bytebuddy.implementation.bind.annotation.Origin;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.implementation.bind.annotation.SuperMethod;
import net.bytebuddy.implementation.bind.annotation.This;

import com.fasterxml.jackson.core.JsonParser;

public interface ProxyConfiguration {


    interface Interceptor {

        Object intercept(Object[] arguments) throws Throwable;

        default Object invokeSuper(Object[] arguments) throws InvocationTargetException, IllegalAccessException {
            final ResolvingDecoder self = getResolvingDecoder();
            return superMethod().invoke(self, arguments);
        }

        Method superMethod();

        ResolvingDecoder getResolvingDecoder();

        default Parser getParser() {
            final ResolvingDecoder decoder = getResolvingDecoder();
            if (decoder instanceof ParserAccessor accessor) {
                return accessor.accessParser();
            }
            throw new UnsupportedOperationException("Illegal method invokation. Decoder does not implement ParserAcessor.");
        }

        default JsonDecoder getJsonDecoder() {
            final ResolvingDecoder decoder = getResolvingDecoder();
            if (decoder instanceof InnerDecoderAccessor accessor) {
                return accessor.accessTypedDecoder();
            }
            throw new UnsupportedOperationException("Illegal method invokation. Decoder does not implement InnerDecoderAccessor.");
        }

        default JsonParser getJsonParser() throws NoSuchFieldException, IllegalAccessException {
            final JsonDecoder decoder = getJsonDecoder();
            final Field parserField = JsonDecoder.class.getDeclaredField("in");
            parserField.setAccessible(true);
            return (JsonParser) parserField.get(decoder);
        }

        default void advanceJsonParser(Symbol symbol) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
            final JsonDecoder decoder = getJsonDecoder();
            final Method advanceMethod = JsonDecoder.class.getDeclaredMethod("advance", Symbol.class);
            advanceMethod.setAccessible(true);
            advanceMethod.invoke(decoder, symbol);
        }

    }


    class InterceptorDispatcher {

        @RuntimeType
        public static Object intercept(
                @This ResolvingDecoder decoder,
                @Origin Method invoked,
                @SuperMethod(nullIfImpossible = true) Method superMethod,
                @AllArguments final Object[] arguments
        ) throws Throwable {
            final String methodName = invoked.getName();
            return switch (methodName) {
                case "relaxedReadBytes" -> new ReadByteArrayInterceptor(decoder, superMethod).intercept(arguments);
                default -> superMethod.invoke(decoder, arguments);
            };
        }
    }
}
