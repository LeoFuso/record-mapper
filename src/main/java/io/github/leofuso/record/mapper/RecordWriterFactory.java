package io.github.leofuso.record.mapper;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

/**
 * A RecordWriterFactory has the capability to instantiate {@link DatumWriter Writers} that can be used to write to
 * {@link org.apache.avro.generic.GenericRecord records} or {@code JSON}-compatible objects.
 */
public interface RecordWriterFactory {

    /**
     * Produces a {@link GenericDatumWriter} from a {@link Schema}.
     */
    GenericDatumWriter<GenericData.Record> produceWriter(Schema schema);

    /**
     * Produces a {@link SpecificDatumWriter} from a {@link Class type}
     */
    <T extends SpecificRecord> SpecificDatumWriter<T> produceWriter(Class<T> type);

}
