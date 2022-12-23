package io.github.leofuso.kafka.json2avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

public interface DatumFactory {

    <T extends GenericRecord> DatumReader<T> makeReader(Schema schema, Class<T> type);

    <T extends GenericRecord> DatumWriter<T> makeWriter(Schema schema, Class<T> type);

}
