import java.util.ArrayList;
import java.util.List;
import java.io.Serializable;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Duration;
import org.joda.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.options.GcpOptions;
import org.apache.beam.sdk.options.GoogleCloudOptions;

public class MyPipeline {
  public static void main(String[] args) {
    // Create the pipeline options and set the project and subscription
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(GoogleCloudOptions.class);
    options.setProject("my-project-id");
    options.setStreaming(true);
    options.setRunner(DataflowRunner.class);
    options.setTempLocation("gs://my-bucket/tmp");

    Pipeline pipeline = Pipeline.create(options);
    String subscription = "projects/my-project-id/subscriptions/my-subscription";
    PCollection<String> input = pipeline.apply("Read from Pub/Sub", PubsubIO.readStrings().fromSubscription(subscription));

    PCollection<KV<String, Double>> keyedValues = input.apply("Extract key-value pairs", ParDo.of(new ExtractKeyValueFn()));

    PCollection<KV<String, Double>> averages = keyedValues.apply("Apply tumbling window", Window.<KV<String, Double>>into(FixedWindows.of(Duration.standardMinutes(1)))
        .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(30))))
        .withAllowedLateness(Duration.ZERO)
        .accumulatingFiredPanes()
        .discardingFiredPanes())
        .apply("Calculate average", Combine.perKey(new AverageFn()));

    PCollectionTuple output = averages.apply("Filter invalid data", ParDo.of(new FilterInvalidDataFn(options.getThreshold()))
        .withOutputTags(OUTPUT_TAG, TupleTagList.of(DEADLETTER_TAG)));
    output.get(DEADLETTER_TAG)
        .apply("Format deadletter output", MapElements.into(TypeDescriptors.strings())
            .via((TableRow row) -> row.toString()))
        .apply("Write to deadletter table", BigQueryIO.writeTableRows()
            .to(String.format("%s.%s.%s", options.getProject(), options.getDeadletterDataset(), options.getDeadletterTable()))
            .withSchema(getDeadletterTableSchema())
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

    output.get(OUTPUT_TAG)
        .apply("Format output", MapElements.into(TypeDescriptor.of(TableRow.class))
            .via((KV<String, Double> kv) -> new TableRow().set("key", kv.getKey()).set("average", kv.getValue())))
        .apply("Write to output table", BigQueryIO.writeTableRows()
            .to(String.format("%s:%s.%s", options.getProject(), options.getOutputDataset(), options.getOutputTable()))
            .withSchema(getOutputTableSchema())
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

    pipeline.run();
  }

  static class ExtractKeyValueFn extends DoFn<String, KV<String, Double>> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      try {
        JsonObject json = new Gson().fromJson(c.element(), JsonObject.class);

                String key = json.get("key").getAsString();
        double value = json.get("value").getAsDouble();
        c.output(KV.of(key, value));
      } catch (Exception e) {
        LOG.error("Error parsing message: " + c.element(), e);
      }
    }
  }

  static class AverageFn extends CombineFn<Double, AverageFn.Accum, Double> {
    public static class Accum implements Serializable {
      double sum = 0;
      long count = 0;
    }

    public Accum createAccumulator() {
      return new Accum();
    }

    public Accum addInput(Accum accum, Double input) {
      accum.sum += input;
      accum.count++;
      return accum;
    }

    public Accum mergeAccumulators(Iterable<Accum> accums) {
      Accum merged = new Accum();
      for (Accum accum : accums) {
        merged.sum += accum.sum;
        merged.count += accum.count;
      }
      return merged;
    }

    public Double extractOutput(Accum accum) {
      return accum.sum / accum.count;
    }
  }

  static class FilterInvalidDataFn extends DoFn<KV<String, Double>, KV<String, Double>> {
    private final double threshold;
    private TupleTag<TableRow> deadletterTag;

    public FilterInvalidDataFn(double threshold) {
      this.threshold = threshold;
      this.deadletterTag = new TupleTag<TableRow>() {};
    }

    @ProcessElement
    public void processElement(ProcessContext c, @OutputTags({@OutputTag(OUTPUT_TAG), @OutputTag(DEADLETTER_TAG)}) OutputReceiver<KV<String, Double>> out) {
      KV<String, Double> kv = c.element();
      if (kv.getValue() > threshold) {
        out.get(OUTPUT_TAG).output(kv);
      } else {
        out.get(DEADLETTER_TAG).output(new TableRow()
            .set("timestamp", c.timestamp().toString())
            .set("key", kv.getKey())
            .set("value", kv.getValue()));
      }
    }
  }

  private static TableSchema getDeadletterTableSchema() {
    List<TableFieldSchema> fields = new ArrayList<>();
    fields.add(new TableFieldSchema().setName("timestamp").setType("TIMESTAMP"));
    fields.add(new TableFieldSchema().setName("key").setType("STRING"));
    fields.add(new TableFieldSchema().setName("value").setType("FLOAT"));
    return new TableSchema().setFields(fields);
  }

  private static TableSchema getOutputTableSchema() {
    List<TableFieldSchema> fields = new ArrayList<>();
    fields.add(new TableFieldSchema().setName("key").setType("STRING"));
    fields.add(new TableFieldSchema().setName("average").setType("FLOAT"));
    return new TableSchema().setFields(fields);
  }


  private static final TupleTag<KV<String, Double>> OUTPUT_TAG = new TupleTag<>();
  private static final TupleTag<TableRow> DEADLETTER_TAG = new TupleTag<>();
}
