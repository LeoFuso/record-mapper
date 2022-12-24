package io.github.leofuso.kafka.json2avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

public interface DatumFactory {

    <T extends SpecificRecord> SpecificDatumReader<T> createReader(Class<T> type);

    <T> GenericDatumReader<T> createReader(Schema schema);

    <T extends SpecificRecord> SpecificDatumWriter<T> createWriter(Class<T> type);

    <T> GenericDatumWriter<T> createWriter(Schema schema);

}
