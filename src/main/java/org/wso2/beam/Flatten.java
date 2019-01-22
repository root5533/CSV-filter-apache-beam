package org.wso2.beam;

import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.ThreadLocalRandom;


public class Flatten
{

    public static class DefaultToCurrentSystemTime implements DefaultValueFactory<Long> {
        @Override
        public Long create(PipelineOptions options) {
            return System.currentTimeMillis();
        }
    }

    public static class DefaultToMinTimestampPlusOneHour implements DefaultValueFactory<Long> {
        @Override
        public Long create(PipelineOptions options) {
            return options.as(CSVOptions.class).getMinTimestampMillis()
                    + Duration.standardMinutes(1).getMillis();
        }
    }

    private interface CSVOptions extends PipelineOptions, StreamingOptions {

        @Description("Set input target")
        @Default.String("/home/tuan/WSO2/CSV-filter-apache-beam/input-small.csv")
        String getInputFile();
        void setInputFile(String value);

        @Description("Set output target")
        @Default.String("/home/tuan/WSO2/outputs/flattenResult")
        String getOutput();
        void setOutput(String value);

        @Description("Minimum randomly assigned timestamp, in milliseconds-since-epoch")
        @Default.InstanceFactory(DefaultToCurrentSystemTime.class)
        Long getMinTimestampMillis();

        void setMinTimestampMillis(Long value);

        @Description("Maximum randomly assigned timestamp, in milliseconds-since-epoch")
        @Default.InstanceFactory(DefaultToMinTimestampPlusOneHour.class)
        Long getMaxTimestampMillis();
        void setMaxTimestampMillis(Long value);

    }

    private static class CheckElement extends DoFn<String, KV<String, String[]>> {

        String[] regions = {"Europe", "Asia", "Middle East and North Africa", "Central America and the Caribbean", "Australia and Oceania", "Sub-Saharan Africa"};

        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<KV<String, String[]>> out) {
            String[] words = element.split(",");
            if (Arrays.asList(regions).contains(words[0].trim())) {
//                System.out.println("region selected : " + words[0]);
                KV<String, String[]> kv = KV.of(words[0], Arrays.copyOfRange(words, 1, words.length));
                out.output(kv);
            }
        }

    }

    static class AddTimestampFn extends DoFn<String, String> {
        private final Instant minTimestamp;
        private final Instant maxTimestamp;
        private int count = 0;

        AddTimestampFn(Instant minTimestamp, Instant maxTimestamp) {
            this.minTimestamp = minTimestamp;
            this.maxTimestamp = maxTimestamp;
        }

        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<String> receiver) {
//            Instant randomTimestamp =
//                    new Instant(
//                            ThreadLocalRandom.current()
//                                    .nextLong(minTimestamp.getMillis(), maxTimestamp.getMillis()));
            /*
             * Concept #2: Set the data element with that timestamp.
             */
            Instant customTimestamp = minTimestamp.plus(Duration.standardSeconds(15 * count));
            ++this.count;
            System.out.println(element + " customTimestamp : " + customTimestamp.toDateTime());
            receiver.outputWithTimestamp(element, new Instant(customTimestamp));
        }
    }

    static class AddTimestampFn2 extends DoFn<String, String> {
        private final Instant minTimestamp;
        private final Instant maxTimestamp;
        private int count = 1;

        AddTimestampFn2(Instant minTimestamp, Instant maxTimestamp) {
            this.minTimestamp = minTimestamp;
            this.maxTimestamp = maxTimestamp;
        }

        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<String> receiver) {
//            Instant randomTimestamp =
//                    new Instant(
//                            ThreadLocalRandom.current()
//                                    .nextLong(minTimestamp.getMillis(), maxTimestamp.getMillis()));
            /*
             * Concept #2: Set the data element with that timestamp.
             */
            Instant customTimestamp = minTimestamp.plus(Duration.standardSeconds(8 * count));
            ++this.count;
            System.out.println(element + " customTimestamp : " + customTimestamp.toDateTime());
            receiver.outputWithTimestamp(element, new Instant(customTimestamp));
        }
    }

    public static class FindKeyValueFn extends SimpleFunction<KV<String, Iterable<String[]>>, String> {

        @Override
        public String apply(KV<String, Iterable<String[]>> input) {
            Iterator<String[]> iter = input.getValue().iterator();
            String suffix = "";
            while (iter.hasNext()) {
                String[] details = iter.next();
                suffix = suffix + details[details.length - 1];
            }
            String result = input.getKey().trim() + "  " + suffix;
            return result;
        }

    }

    private static class CSVFilterRegion extends PTransform<PCollection<String>, PCollection<KV<String, String[]>>> {

        public PCollection<KV<String, String[]>> expand(PCollection<String> lines) {
            PCollection<KV<String, String[]>> filtered = lines.apply(ParDo.of(new CheckElement()));
            return filtered;
        }

    }

    public static void main( String[] args )
    {
        CSVOptions options = PipelineOptionsFactory.fromArgs(args).as(CSVOptions.class);
        options.setRunner(FlinkRunner.class);
        options.setStreaming(true);
        runCSVDemo(options);
    }

    private static void runCSVDemo(CSVOptions options) {

        Pipeline pipe = Pipeline.create(options);
        final Instant minTimestamp = new Instant(options.getMinTimestampMillis());
        final Instant maxTimestamp = new Instant(options.getMaxTimestampMillis());

        PCollection<KV<String, String[]>> collection_1 = pipe.apply("Readfile", TextIO.read().from("/home/tuan/WSO2/inputs/input-small.csv"))
                .apply(ParDo.of(new AddTimestampFn(minTimestamp, maxTimestamp)))
                .apply(Window.into(FixedWindows.of(Duration.standardSeconds(20))))
                .apply(new CSVFilterRegion());

        PCollection<KV<String, String[]>> collection_2 = pipe.apply("Readfile", TextIO.read().from("/home/tuan/WSO2/inputs/test-input.csv"))
                .apply(ParDo.of(new AddTimestampFn2(minTimestamp, maxTimestamp)))
                .apply(Window.into(FixedWindows.of(Duration.standardSeconds(20))))
                .apply(new CSVFilterRegion());

        PCollectionList<KV<String, String[]>> collectionList = PCollectionList.of(collection_1).and(collection_2);
        PCollection<KV<String, String[]>> merged = collectionList.apply("merging_2_collections", org.apache.beam.sdk.transforms.Flatten.pCollections());
        merged.apply(GroupByKey.create()).apply(MapElements.via(new FindKeyValueFn()))
                .apply(TextIO.write().to(options.getOutput()));

        pipe.run();

    }
}
