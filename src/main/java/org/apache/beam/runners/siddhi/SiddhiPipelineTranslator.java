package org.apache.beam.runners.siddhi;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineVisitor.Defaults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SiddhiPipelineTranslator extends Defaults {

    private static final Logger LOG = LoggerFactory.getLogger(SiddhiPipelineTranslator.class);
    private SiddhiRunner runner;

    public void translate(Pipeline pipeline) {
        pipeline.traverseTopologically(this);
    }

}
