package io.github.leofuso.kafka.json2avro.instrument;

import javax.annotation.Nullable;

import java.lang.reflect.UndeclaredThrowableException;
import java.util.concurrent.Callable;
import java.util.function.Function;

import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.ResolvingDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.leofuso.kafka.json2avro.exception.Throwables;
import io.github.leofuso.kafka.json2avro.instrument.decoder.JsonParserAccessor;
import io.github.leofuso.kafka.json2avro.instrument.interceptor.AbstractInterceptor;
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

    @SuppressWarnings("unchecked")
    default <T> T parser(Class<T> type) {
        if (JsonParser.class == type) {
            if (in() instanceof JsonParserAccessor accessor) {
                return (T) accessor.accessJsonParser();
            }
            throw new UndeclaredThrowableException(
                    new IllegalStateException("JsonDecoder is not compatible with JsonParserAccessor type.")
            );
        } else {
            if (self() instanceof ParserAccessor accessor) {
                return (T) accessor.accessParser();
            }
            throw new UndeclaredThrowableException(
                    new IllegalStateException("ResolvingDecoder is not compatible with InnerDecoderAccessor type.")
            );
        }
    }

    default JsonDecoder in() {
        final ResolvingDecoder self = self();
        if (self instanceof InnerDecoderAccessor accessor) {
            return accessor.accessDecoder();
        }
        throw new UndeclaredThrowableException(
                new IllegalStateException("ResolvingDecoder is not compatible with InnerDecoderAccessor type.")
        );
    }

    default ResolvingDecoder self() {
        throw new UnsupportedOperationException("getSelf() was not implemented.");
    }

    Callable<?> getOriginal();

}
