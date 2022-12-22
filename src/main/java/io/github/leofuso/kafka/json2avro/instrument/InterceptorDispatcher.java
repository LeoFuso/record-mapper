package io.github.leofuso.kafka.json2avro.instrument;

import java.lang.reflect.Method;

import org.apache.avro.io.ResolvingDecoder;

import net.bytebuddy.implementation.bind.annotation.AllArguments;
import net.bytebuddy.implementation.bind.annotation.Origin;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.implementation.bind.annotation.This;

public class InterceptorDispatcher {

    /**
     * An additional method name proving a relaxed parser for expected Byte Array values
     */
    public static final String READ_BYTES_REWRITE = "$$__readBytes__rewrite$$";

    /**
     * An additional method name proving a relaxed parser for expected Long values
     */
    public static final String READ_LONG_REWRITE = "$$__readLong__rewrite$$";


    @RuntimeType
    @SuppressWarnings("unused")
    public static Object dispatch(@Origin Method origin,
                                  @This ResolvingDecoder self,
                                  @AllArguments final Object[] arguments) {
        final String methodName = origin.getName();
        return switch (methodName) {
            case READ_BYTES_REWRITE -> ReadBytesInterceptor.intercept(self, arguments);
            case READ_LONG_REWRITE -> ReadLongInterceptor.intercept(self);
            default -> throw new UnsupportedOperationException("Unexpected call on an unknown method.");
        };
    }
}
