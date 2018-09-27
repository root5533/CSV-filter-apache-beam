package org.apache.beam.runners.siddhi;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SiddhiRunner extends PipelineRunner<PipelineResult> {

    private static final Logger LOG = LoggerFactory.getLogger(SiddhiRunner.class);
    private final SiddhiPipelineOptions options;

    public static SiddhiRunner fromOptions(PipelineOptions options) {
        SiddhiPipelineOptions siddhiOptions = PipelineOptionsValidator.validate(SiddhiPipelineOptions.class, options);
        return new SiddhiRunner(siddhiOptions);
    }

    private SiddhiRunner(SiddhiPipelineOptions options) {
        this.options = options;
    }

    @Override
    public PipelineResult run(Pipeline pipeline) {

        //Set metrics
        LOG.info("Executing pipeline with Siddhi");
        //Set execution environment
        LOG.info("Translate pipeline to Siddhi Program");
        //Set translation
        LOG.info("Start execution of pipeline");
        //Pipeline execute
        return null;
    }
}
