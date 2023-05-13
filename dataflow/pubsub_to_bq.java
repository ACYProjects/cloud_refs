import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.gcp.bigquery.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.gcp.bigquery.TableSchema;
import com.google.cloud.dataflow.sdk.io.gcp.pubsub.PubsubIO;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.windowing.AfterWatermark;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PDone;
import com.google.common.collect.ImmutableList;

@Description("The Cloud Pub/Sub topic to read from.")
@Required
String getInputTopic();

void setInputTopic(String value);

@Description("The BigQuery table to write the results to.")
@Required
String getOutputTable();

void setOutputTable(String value);

@Description("The BigQuery table to write invalid data to.")

public class TumblingWindowAveragePipeline {

public static void main(String[] args) throws Exception {
PipelineOptionsFactory.register(TumblingWindowAveragePipelineOptions.class);
TumblingWindowAveragePipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(TumblingWindowAveragePipelineOptions.class);
  
Pipeline pipeline = Pipeline.create(options);

PCollection<String> messages = pipeline
    .apply(PubsubIO.readFromPubSub(options.getInputTopic()));

PCollection<Double> averages = messages
    .apply(ParDo.of(new DoFn<String, Double>() {
      @ProcessElement
      public void processElement(ProcessContext c) {
        String message = c.element();
        String[] parts = message.split(",");
        double average = Double.parseDouble(parts[1]) / Double.parseDouble(parts[0]);
        c.output(average);
      }
    }))
    .apply(Window.into(FixedWindows.of(Duration.standardMinutes(1))));

averages.apply(BigQueryIO.writeTableRows(options.getOutputTable(), new TableSchema()
    .setFields(ImmutableList.of(
        new TableSchema.Field("average", TableSchema.FieldType.FLOAT64)
    ))));

averages.apply(MapElements.via(new DoFn<Double, Double>() {
  @ProcessElement
  public void processElement(ProcessContext c) {
    Double average = c.element();
    if (average < 0) {
      c.output(null);
    } else {
      c.output(average);
    }
  }
})).apply(AfterWatermark.pastEndOfWindow())
    .apply(BigQueryIO.writeTableRows(options.getDeadletterTable(), new TableSchema()
        .setFields(ImmutableList.of(
            new TableSchema.Field("average", TableSchema.FieldType.FLOAT64)
        ))));

pipeline.run();
  
  
