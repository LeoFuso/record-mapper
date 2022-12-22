package io.github.leofuso.kafka.json2avro.instrument;

import net.bytebuddy.implementation.bind.annotation.AllArguments;
import net.bytebuddy.implementation.bind.annotation.Origin;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.implementation.bind.annotation.This;

import org.apache.avro.io.ResolvingDecoder;

import java.lang.reflect.Method;

public class InterceptorDispatcher {

    public static final String READ_BYTES_REWRITE = "$$__readBytes__rewrite$$";

    @RuntimeType
    public static Object dispatch(@Origin Method origin,
                                  @This ResolvingDecoder self,
                                  @AllArguments final Object[] arguments) {
        final String methodName = origin.getName();
        return switch (methodName) {
            case READ_BYTES_REWRITE -> ReadBytesInterceptor.intercept(self, arguments);
            default -> throw new UnsupportedOperationException("Unexpected call on an unknown method.");
        };
    }
}
