package io.github.leofuso.kafka.json2avro.instrument;

import javax.annotation.Nullable;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.concurrent.Callable;
import java.util.function.Function;

import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.ResolvingDecoder;
import org.apache.avro.io.parsing.Parser;
import org.apache.avro.io.parsing.Symbol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.leofuso.kafka.json2avro.exception.Throwables;
import io.github.leofuso.kafka.json2avro.instrument.decoder.JsonDecoderAdvancer;
import io.github.leofuso.kafka.json2avro.instrument.decoder.JsonParserAccessor;
import io.github.leofuso.kafka.json2avro.instrument.resolving.decoder.InnerDecoderAccessor;
import io.github.leofuso.kafka.json2avro.instrument.resolving.decoder.ParserAccessor;

import com.fasterxml.jackson.core.JsonParser;

/**
 * An interceptor object that is responsible for invoking a proxy's method.
 */
public interface Interceptor extends Function<Object[], Object> {

    Logger logger = LoggerFactory.getLogger(Interceptor.class);

    /**
     * Intercepts a method call to a proxy.
     *
     * @param arguments The intercepted method arguments, nullable.
     * @return The method's return value.
     *
     * @throws Throwable If the intercepted method raises an exception.
     */
    Object intercept(@Nullable Object[] arguments) throws Throwable;

    @Override
    default Object apply(@Nullable Object[] objects) {
        try {
            return intercept(objects);
        } catch (final UndeclaredThrowableException e) {
            logger.error("Unhandled exception invokating enhanced method.", e);
            final Callable<?> original = getOriginal();
            return noop(original).apply(objects);
        } catch (final Throwable e) {
            Throwables.rethrowRuntimeException(e);
            return null; /* Unreachable code */
        }
    }

    static <T> Interceptor noop(final Callable<T> original) {
        return new NoopInterceptor<>(original);
    }

    class NoopInterceptor<T> extends AbstractInterceptor<T> {

        private NoopInterceptor(final Callable<T> original) {
            super(original);
        }

        @Override
        public Object intercept(final Object[] ignored) throws Exception {
            return getOriginal().call();
        }

        @Override
        public Object apply(@Nullable final Object[] objects) {
            try {
                return intercept(objects);
            } catch (final Exception e) {
                Throwables.rethrowRuntimeException(e);
                return null; /* Uncheable code */
            }
        }
    }

    default Symbol advanceBoth(Symbol symbol) {
        final ResolvingDecoder self = getSelf();
        final JsonDecoder decoder = getDecoder();

        if (self instanceof ParserAccessor accessor && decoder instanceof JsonDecoderAdvancer advancer) {
            try {
                final Parser parser = accessor.accessParser();
                final Symbol actual = parser.advance(symbol);
                advancer.doAdvance(symbol);
                return actual;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        throw new UndeclaredThrowableException(
                new IllegalStateException(
                        """
                                ResolvingDecoder is not compatible with ParserAccessor type and or \
                                JsonDecoder is not compatible with JsonDecoderAdvancer type.
                                """
                )
        );
    }

    default JsonParser getParser() {
        final JsonDecoder decoder = getDecoder();
        if (decoder instanceof JsonParserAccessor accessor) {
            return accessor.accessJsonParser();
        }
        throw new UndeclaredThrowableException(
                new IllegalStateException("JsonDecoder is not compatible with JsonParserAccessor type.")
        );
    }

    default JsonDecoder getDecoder() {
        final ResolvingDecoder self = getSelf();
        if (self instanceof InnerDecoderAccessor accessor) {
            return accessor.accessDecoder();
        }
        throw new UndeclaredThrowableException(
                new IllegalStateException("ResolvingDecoder is not compatible with InnerDecoderAccessor type.")
        );
    }

    default ResolvingDecoder getSelf() {
        throw new UnsupportedOperationException("getSelf() was not implemented.");
    }

    Callable<?> getOriginal();

}
