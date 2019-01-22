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
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

import java.util.Arrays;
import java.util.Iterator;


public class Partition {

    private interface CustomOptions extends PipelineOptions, StreamingOptions {
        @Description("Set output target")
        @Default.String("/home/tuan/WSO2/outputs/partitionResult")
        String getOutput();
        void setOutput(String value);
    }

    private static class ModFn implements org.apache.beam.sdk.transforms.Partition.PartitionFn<String> {
        @Override
        public int partitionFor(String elem, int numPartitions) {
            return Integer.parseInt(elem) % numPartitions;
        }
    }


    private static void runPartition(CustomOptions options) {
        Pipeline pipe = Pipeline.create(options);
        PCollectionList<String> outputs = pipe.apply(Create.of("1", "2", "3", "4", "5"))
                .apply(org.apache.beam.sdk.transforms.Partition.of(2, new ModFn()));
        PCollection<String> collection = outputs.get(0);
        PCollection<String> collection2 = outputs.get(1);
        collection.apply(TextIO.write().to(options.getOutput() + "1.txt"));
        collection2.apply(TextIO.write().to(options.getOutput() + "2.txt"));
        pipe.run();
    }

    public static void main( String[] args ) {
        CustomOptions options = PipelineOptionsFactory.as(CustomOptions.class);
        options.setRunner(FlinkRunner.class);
        options.setStreaming(true);
        runPartition(options);
    }
}
