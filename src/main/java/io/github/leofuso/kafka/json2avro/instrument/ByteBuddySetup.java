package io.github.leofuso.kafka.json2avro.instrument;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Function;

import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.dynamic.scaffold.MethodGraph;
import net.bytebuddy.implementation.MethodCall;
import net.bytebuddy.implementation.bytecode.assign.Assigner;

import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.ParsingDecoder;
import org.apache.avro.io.ResolvingDecoder;
import org.apache.avro.io.ValidatingDecoder;

import io.github.leofuso.kafka.json2avro.instrument.resolving.decoder.InnerDecoderAccessor;
import io.github.leofuso.kafka.json2avro.instrument.resolving.decoder.ParserAccessor;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.agent.ByteBuddyAgent;
import net.bytebuddy.description.ModifierReviewable;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.loading.ClassReloadingStrategy;
import net.bytebuddy.implementation.FieldAccessor;
import net.bytebuddy.implementation.MethodDelegation;

import static io.github.leofuso.kafka.json2avro.instrument.ProxyConfiguration.InterceptorDispatcher;
import static net.bytebuddy.description.modifier.Visibility.PUBLIC;
import static net.bytebuddy.implementation.bytecode.assign.Assigner.Typing.DYNAMIC;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.returns;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;
import static net.bytebuddy.matcher.ElementMatchers.takesNoArguments;

public class ByteBuddySetup {

    private final ByteBuddy instance = new ByteBuddy().with(MethodGraph.Compiler.Default.forJVMHierarchy());

    private <T> Class<? extends T> reload(Class<T> target, Function<ByteBuddy, DynamicType.Unloaded<? extends T>> instructions) {
        try (final DynamicType.Unloaded<? extends T> unloaded = instructions.apply(instance)) {
            final ClassLoader classLoader = target.getClassLoader();
            final ClassReloadingStrategy classLoadingStrategy = ClassReloadingStrategy.fromInstalledAgent();
            final DynamicType.Loaded<? extends T> dynamicType = unloaded.load(classLoader, classLoadingStrategy);
            return dynamicType.getLoaded();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void init() {

        ByteBuddyAgent.install();

        /* @formatter:off */
        final Class<? extends ResolvingDecoder> resolvingDecoder =
        reload(
                ResolvingDecoder.class,
                byteBuddy -> byteBuddy.subclass(ResolvingDecoder.class)
                        .defineMethod("relaxedReadBytes", Object.class, Visibility.PUBLIC)
                        .withParameter(ByteBuffer.class)
                        .intercept(MethodDelegation.to(InterceptorDispatcher.class))
//                        .method(
//                                named("readBytes")
//                                        .and(returns(ByteBuffer.class))
//                                        .and(takesArguments(ByteBuffer.class))
//                        )
//                        .intercept(MethodCall.invoke(named("relaxedReadBytes")).withAllArguments())
                        .method(
                                named("readLong")
                                        .and(takesNoArguments())
                        )
                            .intercept(MethodDelegation.to(InterceptorDispatcher.class))
                        .implement(ParserAccessor.class)
                            .intercept(
                                    FieldAccessor.ofField("parser")
                                            .in(ParsingDecoder.class)
                            )
                        .implement(InnerDecoderAccessor.class)
                            .intercept(
                                    FieldAccessor.ofField("in")
                                            .in(ValidatingDecoder.class)
                            )
                        .make()
        );

        final Class<? extends DecoderFactory> decoderFactory =
        reload(DecoderFactory.class,
               byteBuddy -> byteBuddy.subclass(DecoderFactory.class)
                       .method(
                               named("resolvingDecoder")
                                       .and(returns(ResolvingDecoder.class))
                       )
                       .intercept(MethodDelegation.toConstructor(resolvingDecoder))
                       .make()
                );

        reload(DecoderFactory.class,
               byteBuddy -> byteBuddy.redefine(DecoderFactory.class)
                       .method(
                               named("get")
                                       .and(takesNoArguments())
                                       .and(ModifierReviewable.OfByteCodeElement::isStatic)
                                       .and(returns(DecoderFactory.class))
                       )
                       .intercept(MethodDelegation.toConstructor(decoderFactory))
                       .make()
        );
        /* @formatter:on */


    }

}
