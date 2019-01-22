package org.wso2.beam;

import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.util.Iterator;


public class Trigger
{

    private interface DefaultOptions extends PipelineOptions, StreamingOptions {

        @Description("Set input target")
        @Default.String("")
        String getInputFile();
        void setInputFile(String value);

        @Description("Set output target")
        @Default.String("")
        String getOutput();
        void setOutput(String value);

    }

    private static class AddTimestampFn extends DoFn<String, KV<String, String>> {
        private Instant minTimestamp;

        AddTimestampFn(Instant minTimestamp) {
            this.minTimestamp = minTimestamp;
        }

        @DoFn.ProcessElement
        public void processElement(@Element String element, OutputReceiver<KV<String, String>> receiver) {
            Instant randomTimestamp = minTimestamp.plus(Duration.standardSeconds(10));
            this.minTimestamp = randomTimestamp;
            receiver.outputWithTimestamp(KV.of("key", element), new Instant(randomTimestamp));
        }
    }

    private static class JoinString extends SimpleFunction<KV<String, Iterable<String>>, String> {

        @Override
        public String apply(KV<String, Iterable<String>> input) {
            Iterator<String> iter = input.getValue().iterator();
            StringBuilder join = new StringBuilder(">");
            while (iter.hasNext()) {
                join.append(iter.next());
            }
            return join.toString();
        }

    }

    private static void runTriggerDemo(PipelineOptions options) {
        Instant timestamp = new Instant();
        Pipeline pipe = Pipeline.create(options);
        PCollection<KV<String, String>> collection = pipe.apply(Create.of("A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M"))
                .apply(ParDo.of(new AddTimestampFn(timestamp)));

        collection.apply(Window.<KV<String, String>>into(FixedWindows.of(Duration.standardSeconds(65)))
                .triggering(AfterWatermark.pastEndOfWindow().withEarlyFirings(AfterPane.elementCountAtLeast(5)))
                .withAllowedLateness(Duration.standardSeconds(0))
                .discardingFiredPanes())
//                .apply(Window.<KV<String, String>>into(FixedWindows.of(Duration.standardSeconds(6)))
//                        .triggering(AfterWatermark.pastEndOfWindow()
//                                .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane()
//                                        .plusDelayOf(Duration.standardSeconds(2)))).accumulatingFiredPanes()
//                        .withAllowedLateness(Duration.standardSeconds(0)))
                .apply(GroupByKey.create())
                .apply(MapElements.via(new JoinString()))
                .apply(TextIO.write().to("/home/tuan/WSO2/outputs/trigger"));
//        pipe.run();


        collection.apply(Window.<KV<String, String>>into(FixedWindows.of(Duration.standardSeconds(65))))
                .apply(GroupByKey.create())
                .apply(MapElements.via(new JoinString()))
                .apply(TextIO.write().to("/home/tuan/WSO2/outputs/window"));

        pipe.run();
    }

    public static void main( String[] args )
    {
        DefaultOptions options = PipelineOptionsFactory.fromArgs(args).as(DefaultOptions.class);
        options.setRunner(FlinkRunner.class);
        options.setStreaming(true);
        runTriggerDemo(options);
    }

}
