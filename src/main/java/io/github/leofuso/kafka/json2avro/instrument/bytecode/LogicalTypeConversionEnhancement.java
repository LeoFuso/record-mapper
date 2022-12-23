package io.github.leofuso.kafka.json2avro.instrument.bytecode;

import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.scaffold.MethodGraph;

public class LogicalTypeConversionEnhancement implements Runnable, Function<ByteBuddy, Void> {

    private static final Logger logger = LoggerFactory.getLogger(LogicalTypeConversionEnhancement.class);

    public static void enhance() {
        logger.trace("Applying LogicalType conversion enhancement using byte code rewriter...");
        final LogicalTypeConversionEnhancement rewriter = new LogicalTypeConversionEnhancement();
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

        } catch (final Exception e) {
            logger.error("Unnable to apply byte code enhancement.", e);
            return null;
        }
    }

    @Override
    public void run() {
        final MethodGraph.Compiler compiler = MethodGraph.Compiler.Default.forJVMHierarchy();
        final ByteBuddy byteBuddy = new ByteBuddy().with(compiler);
        new LogicalTypeConversionEnhancement()
                .apply(byteBuddy);
    }
}
