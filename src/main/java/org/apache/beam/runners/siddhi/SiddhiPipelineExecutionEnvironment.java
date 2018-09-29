package org.apache.beam.runners.siddhi;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;

public class SiddhiPipelineExecutionEnvironment {

    SiddhiPipelineOptions options;

    public SiddhiPipelineExecutionEnvironment(SiddhiPipelineOptions options) {
        this.options = options;
    }

    public void translate(SiddhiRunner runner, Pipeline pipeline) {

        System.out.println("At translate");
        SiddhiPipelineTranslator translator = new SiddhiPipelineTranslator();
        translator.translate(pipeline);

    }

}
