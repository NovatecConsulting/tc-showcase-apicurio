import com.acme.avro.User;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class UserEncoder {

    public byte[] encodeAvroUser(User record, Schema schema) throws IOException {
        GenericDatumWriter<User> specificWriter = new SpecificDatumWriter<>(schema);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        outputStream.reset();
        BinaryEncoder binaryEncoder = new EncoderFactory().binaryEncoder(outputStream, null);
        specificWriter.write(record, binaryEncoder);
        binaryEncoder.flush();

        return outputStream.toByteArray();
    }

    public User decodeAvroUser(byte[] recordBytes) throws IOException {
        SpecificDatumReader<User> datumReader = new SpecificDatumReader<>(User.class);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(recordBytes);
        BinaryDecoder binaryDecoder = new DecoderFactory().binaryDecoder(inputStream, null);

        return datumReader.read(null, binaryDecoder);
    }
}
