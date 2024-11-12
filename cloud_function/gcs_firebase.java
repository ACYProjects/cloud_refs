import com.google.cloud.functions.HttpFunction;
import com.google.cloud.functions.HttpRequest;
import com.google.cloud.functions.HttpResponse;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.firebase.cloud.FirestoreClient;
import com.google.firebase.database.DatabaseReference;
import com.google.firebase.database.FirebaseDatabase;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class ParquetToFirestoreFunction implements HttpFunction {

    private static final String BUCKET_NAME = "";
    private static final String FIRESTORE_COLLECTION = "";

    @Override
    public void service(HttpRequest request, HttpResponse response) throws IOException, ExecutionException, InterruptedException {
        String filePath = request.getParameter("filePath");

        GenericRecord record = readParquetFile(filePath);

        ValidationResult validationResult = validateRecord(record);
        if (validationResult.isValid()) {
            writeToFirestore(record, validationResult.getValidationTimestamp());
            response.setStatusCode(200);
            response.getWriter().write("Data processed and written to Firestore successfully.");
        } else {
            response.setStatusCode(400);
            response.getWriter().write("Invalid data in the Parquet file: " + validationResult.getErrorMessage());
        }
    }

    private GenericRecord readParquetFile(String filePath) throws IOException {
        Storage storage = StorageOptions.getDefaultInstance().getService();
        Blob blob = storage.get(BlobId.of(BUCKET_NAME, filePath));
        try (InputStream inputStream = blob.getContent()) {
            Configuration conf = new Configuration();
            ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(new Path(filePath))
                    .withConf(conf)
                    .build();
            return reader.read();
        }
    }

    private ValidationResult validateRecord(GenericRecord record) {
        if (record.get("name") == null || record.get("name").toString().isEmpty()) {
            return ValidationResult.invalid("Name is required.");
        }

        if (record.get("age") == null || ((int) record.get("age")) < 0) {
            return ValidationResult.invalid("Age must be a non-negative integer.");
        }

        if (record.get("salary") == null || ((double) record.get("salary")) < 0) {
            return ValidationResult.invalid("Salary must be a non-negative number.");
        }

        return ValidationResult.valid(Instant.now());
    }

    private void writeToFirestore(GenericRecord record, Instant validationTimestamp) throws ExecutionException, InterruptedException {
        FirebaseDatabase database = FirebaseDatabase.getInstance();
        DatabaseReference ref = database.getReference(FIRESTORE_COLLECTION);

        Map<String, Object> data = new HashMap<>();
        for (org.apache.avro.Schema.Field field : record.getSchema().getFields()) {
            data.put(field.name(), record.get(field.name()));
        }
        data.put("validationTimestamp", validationTimestamp.toString());

        ref.child(ByteBuffer.wrap(record.hashCode()).getLong()).setValueAsync(data).get();
    }

    private static class ValidationResult {
        private final boolean valid;
        private final String errorMessage;
        private final Instant validationTimestamp;

        private ValidationResult(boolean valid, String errorMessage, Instant validationTimestamp) {
            this.valid = valid;
            this.errorMessage = errorMessage;
            this.validationTimestamp = validationTimestamp;
        }

        public static ValidationResult valid(Instant validationTimestamp) {
            return new ValidationResult(true, null, validationTimestamp);
        }

        public static ValidationResult invalid(String errorMessage) {
            return new ValidationResult(false, errorMessage, null);
        }

        public boolean isValid() {
            return valid;
        }

        public String getErrorMessage() {
            return errorMessage;
        }

        public Instant getValidationTimestamp() {
            return validationTimestamp;
        }
    }
}
