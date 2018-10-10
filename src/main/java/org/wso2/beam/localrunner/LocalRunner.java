package org.wso2.beam.localrunner;

import org.apache.beam.runners.siddhi.SiddhiPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalRunner extends PipelineRunner<PipelineResult> {

    private static final Logger LOG = LoggerFactory.getLogger(LocalRunner.class);
    private final LocalPipelineOptions options;

    public static LocalRunner fromOptions(PipelineOptions options) {
        LocalPipelineOptions localOptions = PipelineOptionsValidator.validate(LocalPipelineOptions.class, options);
        return new LocalRunner(localOptions);
    }

    private LocalRunner(LocalPipelineOptions options) {
        this.options = options;
    }

    @Override
    public PipelineResult run(Pipeline pipeline) {
        LOG.info("Executing local runner");
        LocalGraphVisitor graphVisitor = new LocalGraphVisitor();
        pipeline.traverseTopologically(graphVisitor);
        DirectGraph graph = graphVisitor.getGraph();
        return null;
    }

}