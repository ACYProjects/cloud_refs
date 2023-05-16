import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Duration;

import java.util.regex.Pattern;

public class KafkaToBigQueryPipeline {

    public interface Options extends PipelineOptions {
        @Description("Kafka bootstrap servers")
        @Default.String("localhost:9092")
        String getBootstrapServers();
        void setBootstrapServers(String value);

        @Description("Kafka topic")
        @Default.String("my-topic")
        String getTopic();
        void setTopic(String value);

        @Description("BigQuery table specification")
        @Validation.Required
        String getTableSpec();
        void setTableSpec(String value);

        @Description("BigQuery dead letter table specification")
        @Validation.Required
        String getDeadLetterTableSpec();
        void setDeadLetterTableSpec(String value);
    }

    static class DataValidationDoFn extends DoFn<String, String> {
        private static final Pattern NAME_REGEX_PATTERN = Pattern.compile("^[a-zA-Z ]+$");

        private final TupleTag<String> validDataTag;
        private final TupleTag<String> invalidDataTag;

        public DataValidationDoFn(TupleTag<String> validDataTag, TupleTag<String> invalidDataTag) {
            this.validDataTag = validDataTag;
            this.invalidDataTag = invalidDataTag;
        }

        @ProcessElement
        public void processElement(ProcessContext c, BoundedWindow window) {
            String data = c.element();
            String[] fields = data.split(",");

            if (fields.length != 3) {
                // Invalid data format
                c.output(invalidDataTag, data);
                return;
            }

            String id = fields[0];
            String name = fields[1];
            String age = fields[2];

            if (!isInteger(id)) {
                c.output(invalidDataTag, "Invalid data type for id: " + id);
            } else if (!isString(name)) {
                c.output(invalidDataTag, "Invalid data type for name: " + name);
            } else if (!isInteger(age)) {
                c.output(invalidDataTag, "Invalid data type for age: " + age);
            }

            if (!NAME_REGEX_PATTERN.matcher(name).matches()) {
                c.output(invalidDataTag, "Invalid name format: " + name);
            }

            c.output(validDataTag, data);
        }

        private boolean isInteger(String value) {
            try {
                Integer.parseInt(value);
                return true;
            } catch (NumberFormatException e) {
                return false;
            }
        }

        private boolean isString(String value) {
            return value != null && !value.isEmpty();
        }
    }

    public static void main(String[] args) {
        Options options = PipelineOptionsFactory.fromArgs
                          .withValidation()
                .as(Options.class);

        Pipeline pipeline = Pipeline.create(options);

        final TupleTag<String> validDataTag = new TupleTag<>();
        final TupleTag<String> invalidDataTag = new TupleTag<>();

        PCollection<String> input = pipeline
                .apply(KafkaIO.<String, String>read()
                        .withBootstrapServers(options.getBootstrapServers())
                        .withTopic(options.getTopic())
                        .withoutMetadata())
                .apply(Values.<String>create());

        PCollectionTuple validatedData = input
                .apply(Window.<String>into(FixedWindows.of(Duration.standardMinutes(5)))
                        .withAllowedLateness(Duration.ZERO)
                        .withTimestampCombiner(Window.TimestampCombiner.END_OF_WINDOW))
                .apply(ParDo.of(new DataValidationDoFn(validDataTag, invalidDataTag))
                        .withSideInputs(/* Add side inputs here if necessary */)
                        .withOutputTags(validDataTag, TupleTagList.of(invalidDataTag)));

        PCollection<String> validData = validatedData.get(validDataTag);
        PCollection<String> invalidData = validatedData.get(invalidDataTag);

        validData.apply("WriteToBigQuery", BigQueryIO.write()
                .to(options.getTableSpec())
                .withSchema(/* Define the BigQuery schema here */)
                .withWriteDisposition(Write.WriteDisposition.WRITE_APPEND)
                .withCreateDisposition(Write.CreateDisposition.CREATE_IF_NEEDED));

        invalidData.apply("WriteInvalidToBigQuery", BigQueryIO.write()
                .to(options.getDeadLetterTableSpec())
                .withSchema(/* Define the dead letter BigQuery schema here */)
                .withWriteDisposition(Write.WriteDisposition.WRITE_APPEND)
                .withCreateDisposition(Write.CreateDisposition.CREATE_IF_NEEDED));

        pipeline.run();
    }
}


    
