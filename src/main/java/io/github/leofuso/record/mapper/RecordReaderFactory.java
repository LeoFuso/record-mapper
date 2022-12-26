package io.github.leofuso.record.mapper;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;

/**
 * A RecordReaderFactory has the capability to instantiate {@link DatumReader Readers} that can be used to read from
 * {@link org.apache.avro.generic.GenericRecord records} or {@code JSON}-compatible objects.
 */
public interface RecordReaderFactory {

    /**
     * Produces a {@link GenericDatumReader} from a {@link Schema}.
     */
    GenericDatumReader<GenericData.Record> produceReader(Schema schema);

    /**
     * Produces a {@link SpecificDatumReader} from a {@link Class type}
     */
    <T extends SpecificRecord> SpecificDatumReader<T> produceReader(Class<T> type);

}
