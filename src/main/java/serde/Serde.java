package serde;

import com.acme.avro.User;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import serde.ProducerApplication;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.logging.Logger;

public class Serde {
    private static final Logger log = Logger.getLogger(ProducerApplication.class.getName());

    public <T extends GenericRecord> byte[] encodeToAvro(T record) throws IOException {
        GenericDatumWriter<T> specificWriter = new GenericDatumWriter<>();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        outputStream.reset();
        BinaryEncoder binaryEncoder = new EncoderFactory().binaryEncoder(outputStream, null);
        specificWriter.setSchema(record.getSchema());
        specificWriter.write(record, binaryEncoder);
        binaryEncoder.flush();

        return outputStream.toByteArray();
    }

    public <T extends SpecificRecord> T decodeToAvro(byte[] recordBytes, Class<T> recordClass) throws IOException {
        SpecificDatumReader<T> datumReader = new SpecificDatumReader<>(recordClass);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(recordBytes);
        BinaryDecoder binaryDecoder = new DecoderFactory().binaryDecoder(inputStream, null);

        return datumReader.read(null, binaryDecoder);
    }

    public GenericRecord decodeAvroToGeneric(byte[] recordBytes, Schema schema) throws IOException {
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        Decoder decoder = DecoderFactory.get().binaryDecoder(recordBytes, null);

        return datumReader.read(null, decoder);
    }
}
