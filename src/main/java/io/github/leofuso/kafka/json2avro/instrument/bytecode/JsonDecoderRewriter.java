package io.github.leofuso.kafka.json2avro.instrument.bytecode;

import java.util.function.Function;

import org.apache.avro.io.parsing.Symbol;

import io.github.leofuso.kafka.json2avro.instrument.decoder.JsonDecoderAdvancer;
import io.github.leofuso.kafka.json2avro.instrument.decoder.JsonParserAccessor;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.ClassFileLocator;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.FieldAccessor;
import net.bytebuddy.implementation.MethodCall;
import net.bytebuddy.pool.TypePool;

import static net.bytebuddy.matcher.ElementMatchers.isDeclaredBy;
import static net.bytebuddy.matcher.ElementMatchers.isPrivate;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.not;
import static net.bytebuddy.matcher.ElementMatchers.takesArgument;

public class JsonDecoderRewriter implements Function<ByteBuddy, DynamicType.Loaded<?>> {

    @Override
    public DynamicType.Loaded<?> apply(final ByteBuddy byteBuddy) {

        final ClassLoader classLoader = ClassLoader.getSystemClassLoader();
        final ClassFileLocator classFileLocator = ClassFileLocator.ForClassLoader.of(classLoader);
        final TypePool typePool = TypePool.Default.of(classFileLocator);

        final TypeDescription jsonDecoderType =
                typePool.describe("org.apache.avro.io.JsonDecoder")
                        .resolve();

        return byteBuddy.rebase(jsonDecoderType, classFileLocator)
                .implement(JsonParserAccessor.class)
                .intercept(
                        FieldAccessor.ofField("in")
                                .in(jsonDecoderType)
                )
                .implement(JsonDecoderAdvancer.class)
                .intercept(
                        MethodCall.invoke(
                                        named("advance")
                                                .and(takesArgument(0, Symbol.class))
                                                .and(isPrivate())
                                                .and(not(isDeclaredBy(JsonDecoderAdvancer.class)))
                                )
                                .withAllArguments()
                )
                .make()
                .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
    }
}
