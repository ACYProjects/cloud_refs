import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;

public class ParseDataFn extends DoFn<String, KV<String, Double>> {
    @ProcessElement
    public void processElement(ProcessContext context) {
        String input = context.element();
        // Assuming input is in the format "key,value"
        String[] parts = input.split(",");
        if (parts.length == 2) {
            try {
                String key = parts[0];
                Double value = Double.parseDouble(parts[1]);
                context.output(KV.of(key, value));
            } catch (NumberFormatException e) {
                // Handle parsing errors
            }
        }
    }
}

public class IsValidDataFn implements SerializableFunction<KV<String, Double>, Boolean> {
    @Override
    public Boolean apply(KV<String, Double> input) {
        // Add your validation logic here
        // For example, check if the value is within a specific range
        return input.getValue() >= 0 && input.getValue() <= 100;
    }
}

public class IsInvalidDataFn implements SerializableFunction<KV<String, Double>, Boolean> {
    @Override
    public Boolean apply(KV<String, Double> input) {
        // Reuse IsValidDataFn logic and negate the result
        IsValidDataFn isValidDataFn = new IsValidDataFn();
        return !isValidDataFn.apply(input);
    }
}


public class DataflowPipeline {
    public static void main(String[] args) {
        PipelineOptionsFactory.register(GoogleCloudOptions.class);
        GoogleCloudOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(GoogleCloudOptions.class);
        options.setStreaming(true);
        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> input = pipeline.apply("ReadFromPubSub", PubsubIO.readStrings().fromTopic(options.getPubsubTopic()));

        PCollection<KV<String, Double>> parsedData = input.apply("ParseData", ParDo.of(new ParseDataFn()));
        PCollection<KV<String, Double>> validData = parsedData.apply("FilterValidData", Filter.by(new IsValidDataFn()));
        PCollection<KV<String, Double>> invalidData = parsedData.apply("FilterInvalidData", Filter.by(new IsInvalidDataFn()));

        PCollection<KV<String, Double>> windowedData = validData.apply("ApplyTumblingWindow", Window.<KV<String, Double>>into(FixedWindows.of(Duration.standardMinutes(1)))
                .withAllowedLateness(Duration.standardMinutes(1))
                .triggering(AfterWatermark.pastEndOfWindow())
                .accumulatingFiredPanes()
                .withTimestampCombiner(TimestampCombiner.END_OF_WINDOW));

        PCollection<KV<String, Double>> averages = windowedData.apply("CalculateAverage", Mean.perKey());

        averages.apply("WriteToBigQuery", BigQueryIO.writeTableRows()
                .to(options.getOutputTable())
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        invalidData.apply("WriteDeadletterToBigQuery", BigQueryIO.writeTableRows()
                .to(options.getDeadletterTable())
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        pipeline.run();
    }
}
