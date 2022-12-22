package io.github.leofuso.kafka.json2avro.instrument;

import java.util.Objects;
import java.util.concurrent.Callable;

import org.apache.avro.io.ResolvingDecoder;

public abstract class AbstractInterceptor<T> implements Interceptor {

    private final Callable<T> original;
    private final ResolvingDecoder self;

    protected AbstractInterceptor(final Callable<T> original) {
        this.original = Objects.requireNonNull(original, "Callable<T> [original] is required.");
        this.self = null;
    }

    protected AbstractInterceptor(final ResolvingDecoder self, final Callable<T> original) {
        this.original = Objects.requireNonNull(original, "Callable<T> [original] is required.");
        this.self = Objects.requireNonNull(self, "ResolvingDecoder [self] is required.");
    }

    @Override
    public ResolvingDecoder getSelf() {
        return self;
    }

    @Override
    public Callable<?> getOriginal() {
        return original;
    }
}
