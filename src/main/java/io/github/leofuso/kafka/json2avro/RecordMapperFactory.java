package io.github.leofuso.kafka.json2avro;

import org.apache.avro.Conversion;

import io.github.leofuso.kafka.json2avro.internal.DefaultRecordMapperFactory;
import io.github.leofuso.kafka.json2avro.internal.EnhancedRecordMapperFactory;

/**
 * A RecordMapperFactory has the capability of producing new {@link RecordMapper} instances.
 */
public interface RecordMapperFactory {

    /**
     * @return a {@link RecordMapperFactory factory} capable of producing {@link RecordMapper} instances.
     * <p>
     * If the ByteBuddy dependency is present in the classpath,
     * a byte bode instrumentation is applied into the {@code JSON} parsing process.
     * <p>
     * The instrumentation offers a relaxed-binding for some {@link org.apache.avro.LogicalType LogicalTypes} conversions.
     */
    static RecordMapperFactory get() {
        try {
            Class.forName("net.bytebuddy.ByteBuddy");
            return new EnhancedRecordMapperFactory();
        } catch (ClassNotFoundException ignored) {
            return new DefaultRecordMapperFactory();
        }
    }

    /**
     * @return a new instance of the {@link RecordMapper} type with {@code additional} {@link Conversion conversions}, if provided.
     */
    RecordMapper produce(Conversion<?>... additional);

}
