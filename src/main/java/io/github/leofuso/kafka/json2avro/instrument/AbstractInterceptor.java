package io.github.leofuso.kafka.json2avro.instrument;

import org.apache.avro.io.ResolvingDecoder;

import java.lang.reflect.Method;

public abstract class AbstractInterceptor implements ProxyConfiguration.Interceptor {

    private final ResolvingDecoder self;
    private final Method superMethod;

    protected AbstractInterceptor(final ResolvingDecoder self, final Method superMethod) {
        this.self = self;
        this.superMethod = superMethod;
    }


    @Override
    public Method superMethod() {
        return superMethod;
    }

    @Override
    public ResolvingDecoder getResolvingDecoder() {
        return self;
    }

}
