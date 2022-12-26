package io.github.leofuso.record.mapper.instrument;

import javax.annotation.Nullable;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.function.Function;

import io.github.leofuso.record.mapper.exception.Throwables;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.ResolvingDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An interceptor object that is responsible for invoking a proxy's method.
 */
public interface Interceptor extends Function<Object[], Object> {

    Logger logger = LoggerFactory.getLogger(Interceptor.class);

    static <I extends Interceptor> Object intercept(Class<I> strategy, final ResolvingDecoder self, final Object[] arguments) {
        try {
            final Constructor<I> constructor = strategy.getDeclaredConstructor(ResolvingDecoder.class, Object[].class);
            final I interceptor = constructor.newInstance(self, arguments);
            return interceptor.apply(arguments);
        } catch (InvocationTargetException | NoSuchMethodException | InstantiationException | IllegalAccessException e) {
            Throwables.handleReflectionException(e);
            return null; /* Unreacheable code */
        }
    }

    /**
     * Intercepts a method call to a proxy.
     *
     * @param arguments The intercepted method arguments, nullable.
     * @return The method's return value.
     *
     * @throws Throwable If the intercepted method raises an exception.
     */
    Object intercept(@Nullable Object[] arguments) throws Throwable;

    class NoopInterceptor<T> implements Interceptor {

        private final Callable<T> invoked;

        public NoopInterceptor(final Callable<T> invoked) {
            this.invoked = Objects.requireNonNull(invoked, "Callable<T> [invoked] is required.");
        }

        @Override
        public Object intercept(final Object[] ignored) throws Exception {
            return invoked().call();
        }

        @Override
        public Object apply(@Nullable final Object[] arguments) {
            try {
                return intercept(arguments);
            } catch (final Exception e) {
                Throwables.rethrowRuntimeException(e);
                return null; /* Uncheable code */
            }
        }

        @Override
        public Callable<?> invoked() {
            return invoked;
        }
    }

    default <T> T parser(Class<T> type) {
        throw new UnsupportedOperationException("parser(Class) was not implemented.");
    }

    default JsonDecoder in() {
        throw new UnsupportedOperationException("in() was not implemented.");
    }

    default ResolvingDecoder self() {
        throw new UnsupportedOperationException("self() was not implemented.");
    }

    Callable<?> invoked();

}
