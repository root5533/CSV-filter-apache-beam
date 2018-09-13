package org.wso2.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import sun.java2d.pipe.SpanShapeRenderer;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Hello world!
 *
 */
public class App 
{

    private interface CSVOptions extends PipelineOptions {

        @Description("Set input target")
        @Default.String("input-small.csv")
        String getInputFile();
        void setInputFile(String value);

        @Description("Set output target")
        @Default.String("output")
        String getOutput();
        void setOutput(String value);

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

    public static class FindKeyValueFn extends SimpleFunction<KV<String, Iterable<String[]>>, String> {

        @Override
        public String apply(KV<String, Iterable<String[]>> input) {
            Iterator<String[]> iter = input.getValue().iterator();
            float total_profit = 0;
            while (iter.hasNext()) {
                String[] details = iter.next();
                total_profit += Float.parseFloat(details[details.length - 1]) / 1000000;
            }
            String result = input.getKey().trim() + " region profits : " + total_profit + " million";
//            System.out.println(result);
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
        runCSVDemo(options);
    }

    private static void runCSVDemo(CSVOptions options) {

        Pipeline pipe = Pipeline.create(options);
        pipe.apply("ReadFile", TextIO.read().from(options.getInputFile()))
                .apply(new CSVFilterRegion())
                .apply(GroupByKey.<String, String[]>create())
                .apply(MapElements.via(new FindKeyValueFn()))
                .apply("Result", TextIO.write().to("Final"));
        pipe.run();

    }
}
