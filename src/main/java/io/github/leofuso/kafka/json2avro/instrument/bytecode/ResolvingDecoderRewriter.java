package io.github.leofuso.kafka.json2avro.instrument.bytecode;

import java.util.function.Function;

import io.github.leofuso.kafka.json2avro.instrument.interceptor.accessors.DecoderAccessor;
import io.github.leofuso.kafka.json2avro.instrument.interceptor.accessors.ParserAccessor;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.ClassFileLocator;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.FieldAccessor;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.pool.TypePool;

import static io.github.leofuso.kafka.json2avro.instrument.InterceptorDispatcher.READ_BYTES_REWRITE;
import static io.github.leofuso.kafka.json2avro.instrument.InterceptorDispatcher.READ_INT_REWRITE;
import static io.github.leofuso.kafka.json2avro.instrument.InterceptorDispatcher.READ_LONG_REWRITE;

public class ResolvingDecoderRewriter implements Function<ByteBuddy, DynamicType.Loaded<?>> {

    @Override
    public DynamicType.Loaded<?> apply(final ByteBuddy byteBuddy) {

        final ClassLoader classLoader = ClassLoader.getSystemClassLoader();
        final ClassFileLocator classFileLocator = ClassFileLocator.ForClassLoader.of(classLoader);
        final TypePool typePool = TypePool.Default.of(classFileLocator);

        final TypeDescription resolvingDecoderType =
                typePool.describe("org.apache.avro.io.ResolvingDecoder")
                        .resolve();

        final TypeDescription validatingDecoderType =
                typePool.describe("org.apache.avro.io.ValidatingDecoder")
                        .resolve();

        final TypeDescription parsingDecoderType =
                typePool.describe("org.apache.avro.io.ParsingDecoder")
                        .resolve();

        final TypeDescription dispatcherType =
                typePool.describe("io.github.leofuso.kafka.json2avro.instrument.InterceptorDispatcher")
                        .resolve();

        return byteBuddy.rebase(resolvingDecoderType, classFileLocator)
                .implement(ParserAccessor.class)
                .intercept(
                        FieldAccessor.ofField("parser")
                                .in(parsingDecoderType)
                )
                .implement(DecoderAccessor.class)
                .intercept(
                        FieldAccessor.ofField("in")
                                .in(validatingDecoderType)
                )
                .defineMethod(READ_BYTES_REWRITE, Object.class, Visibility.PUBLIC)
                .intercept(MethodDelegation.to(dispatcherType))
                .defineMethod(READ_LONG_REWRITE, Object.class, Visibility.PUBLIC)
                .intercept(MethodDelegation.to(dispatcherType))
                .defineMethod(READ_INT_REWRITE, Object.class, Visibility.PUBLIC)
                .intercept(MethodDelegation.to(dispatcherType))
                .make()
                .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
    }

}
