package io.github.leofuso.record.mapper.instrument.interceptor;

import javax.annotation.Nullable;

import java.lang.reflect.UndeclaredThrowableException;
import java.util.Objects;
import java.util.concurrent.Callable;

import io.github.leofuso.record.mapper.exception.Throwables;
import io.github.leofuso.record.mapper.instrument.Interceptor;
import io.github.leofuso.record.mapper.instrument.interceptor.accessors.DecoderAccessor;
import io.github.leofuso.record.mapper.instrument.interceptor.accessors.JsonParserAccessor;
import io.github.leofuso.record.mapper.instrument.interceptor.accessors.ParserAccessor;
import io.github.leofuso.record.mapper.instrument.interceptor.accessors.ParsingAdvancer;

import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.ResolvingDecoder;
import org.apache.avro.io.parsing.Symbol;

import com.fasterxml.jackson.core.JsonParser;

public abstract class AbstractInterceptor<R> implements Interceptor {

    private final Callable<R> invoked;
    private final ResolvingDecoder self;

    protected AbstractInterceptor(final ResolvingDecoder self, final Callable<R> invoked) {
        this.invoked = Objects.requireNonNull(invoked, "Callable<T> [invoked] is required.");
        this.self = Objects.requireNonNull(self, "ResolvingDecoder [self] is required.");
    }

    @Override
    public Object apply(@Nullable Object[] arguments) {
        try {
            return intercept(arguments);
        } catch (final UndeclaredThrowableException e) {
            logger.error("Unhandled exception invokating enhanced method.", e);
            final Callable<?> invoked = invoked();
            final NoopInterceptor<?> noop = new NoopInterceptor<>(invoked);
            return noop.apply(arguments);
        } catch (final Throwable e) {
            Throwables.rethrowRuntimeException(e);
            return null; /* Unreachable code */
        }
    }

    @Override
    public ResolvingDecoder self() {
        return self;
    }

    @Override
    public Callable<?> invoked() {
        return invoked;
    }

    protected void advance(final Symbol symbol) {
        final JsonDecoder in = in();
        if (in instanceof ParsingAdvancer advancer) {
            advancer.doAdvance(symbol);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <P> P parser(Class<P> parser) {
        if (JsonParser.class == parser) {
            if (in() instanceof JsonParserAccessor accessor) {
                return (P) accessor.accessJsonParser();
            }
            throw new UndeclaredThrowableException(
                    new IllegalStateException("JsonDecoder is not compatible with JsonParserAccessor type.")
            );
        } else {
            if (self() instanceof ParserAccessor accessor) {
                return (P) accessor.accessParser();
            }
            throw new UndeclaredThrowableException(
                    new IllegalStateException("ResolvingDecoder is not compatible with ParserAccessor type.")
            );
        }
    }

    @Override
    public JsonDecoder in() {
        final ResolvingDecoder self = self();
        if (self instanceof DecoderAccessor accessor) {
            return accessor.accessDecoder();
        }
        throw new UndeclaredThrowableException(
                new IllegalStateException("ResolvingDecoder is not compatible with DecoderAccessor type.")
        );
    }
}
