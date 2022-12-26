package io.github.leofuso.kafka.json2avro.instrument;

import java.lang.reflect.Method;

import org.apache.avro.io.ResolvingDecoder;

import io.github.leofuso.kafka.json2avro.instrument.interceptor.EnhancedReadBytes;
import io.github.leofuso.kafka.json2avro.instrument.interceptor.EnhancedReadInt;
import io.github.leofuso.kafka.json2avro.instrument.interceptor.EnhancedReadLong;

import net.bytebuddy.implementation.bind.annotation.AllArguments;
import net.bytebuddy.implementation.bind.annotation.Origin;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.implementation.bind.annotation.This;

/**
 * A static interceptor that dispatches method calls to the right interceptor.
 */
public class InterceptorDispatcher {

    /**
     * An additional method name proving an enhanced parser for expected Byte Array values
     */
    public static final String READ_BYTES_REWRITE = "$$__readBytes__rewrite__$$";

    /**
     * An additional method name proving an enhanced parser for expected Long values
     */
    public static final String READ_LONG_REWRITE = "$$__readLong__rewrite__$$";

    /**
     * An additional method name proving an enhanced parser for expected Integer values
     */
    public static final String READ_INT_REWRITE = "$$__readInt__rewrite__$$";


    /**
     * Intercepts a method call to a proxy.
     *
     * @param invoked   The invoked method.
     * @param self      The proxied instance.
     * @param arguments The method arguments.
     * @return The intercepted method's return value.
     */
    @RuntimeType
    @SuppressWarnings("unused")
    public static Object dispatch(@Origin Method invoked,
                                  @This ResolvingDecoder self,
                                  @AllArguments final Object[] arguments) {
        final String methodName = invoked.getName();
        return switch (methodName) {
            case READ_INT_REWRITE -> Interceptor.intercept(EnhancedReadInt.class, self, arguments);
            case READ_LONG_REWRITE -> Interceptor.intercept(EnhancedReadLong.class, self, arguments);
            case READ_BYTES_REWRITE -> Interceptor.intercept(EnhancedReadBytes.class, self, arguments);
            default -> throw new UnsupportedOperationException("Unexpected call on an unknown method.");
        };
    }
}
