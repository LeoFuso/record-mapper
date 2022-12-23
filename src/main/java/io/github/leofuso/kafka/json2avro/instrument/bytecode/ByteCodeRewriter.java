package io.github.leofuso.kafka.json2avro.instrument.bytecode;

import java.io.IOException;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.scaffold.MethodGraph;

public class ByteCodeRewriter implements Runnable, Function<ByteBuddy, Void> {

    private static final Logger logger = LoggerFactory.getLogger(ByteCodeRewriter.class);

    public static void rewrite() {
        logger.trace("Applying ByteCode rewriter...");
        final ByteCodeRewriter rewriter = new ByteCodeRewriter();
        rewriter.run();
    }

    @Override
    public Void apply(final ByteBuddy byteBuddy) {

        final JsonDecoderRewriter jsonDecoderRewriter = new JsonDecoderRewriter();
        final ResolvingDecoderRewriter resolvingDecoderRewriter = new ResolvingDecoderRewriter();

        try (
                DynamicType.Loaded<?> jsonDecoder = jsonDecoderRewriter.apply(byteBuddy);
                DynamicType.Loaded<?> resolvingDecoder = resolvingDecoderRewriter.apply(byteBuddy)
        ) {

            final TypeDescription jsonDecoderTypeDescription = jsonDecoder.getTypeDescription();
            final String jsonDecoderName = jsonDecoderTypeDescription.getName();
            logger.trace("Enhancement successfully applied to [{}]", jsonDecoderName);

            final TypeDescription resolvingDecoderTypeDescription = resolvingDecoder.getTypeDescription();
            final String resolvingDecoderName = resolvingDecoderTypeDescription.getName();
            logger.trace("Enhancement successfully applied to [{}]", resolvingDecoderName);

            return null;

        } catch (IOException e) {
            logger.error("Unnable to enhance Decoders.", e);
            return null;
        }
    }

    @Override
    public void run() {
        final MethodGraph.Compiler compiler = MethodGraph.Compiler.Default.forJVMHierarchy();
        final ByteBuddy byteBuddy = new ByteBuddy().with(compiler);
        new ByteCodeRewriter()
                .apply(byteBuddy);
    }
}
