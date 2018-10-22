package org.wso2.beam;

import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.wso2.beam.localrunner.LocalPipelineOptions;
import org.wso2.beam.localrunner.LocalRunner;

public class SimpleLocalRunner {

    private interface LocalOptions extends LocalPipelineOptions, StreamingOptions {
        @Description("Set input target")
        @Default.String("/home/tuan/WSO2/CSV-filter-apache-beam/simple.txt")
        String getInputFile();
        void setInputFile(String value);

        @Description("Set output target")
        @Default.String("/home/tuan/WSO2/CSV-filter-apache-beam/output/result")
        String getOutput();
        void setOutput(String value);
    }

    private static class LetterCount extends DoFn<String, String> {
        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<String> out) {
            int count = element.length();
            String output = String.valueOf(count);
            out.output(output);
        }
    }

    private static void runSimpleLocalApp(LocalOptions options) {
        Pipeline pipe = Pipeline.create(options);
        PCollection<String> col1 = pipe.apply("Readfile", TextIO.read().from(options.getInputFile()));
        PCollection<String> col2 = col1.apply("PardoTransform", ParDo.of(new LetterCount()));
        col2.apply("Writefile", TextIO.write().to(options.getOutput()));
        pipe.run();
    }

    public static void main(String[] args) {
        LocalOptions options = PipelineOptionsFactory.fromArgs(args).as(LocalOptions.class);
        options.setRunner(LocalRunner.class);
        runSimpleLocalApp(options);
    }


}
