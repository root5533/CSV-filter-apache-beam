package org.apache.beam.runners.siddhi;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineVisitor.Defaults;
import org.apache.beam.sdk.transforms.PTransform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;

public class SiddhiPipelineTranslator extends Defaults {

    private static final Logger LOG = LoggerFactory.getLogger(SiddhiPipelineTranslator.class);
    private SiddhiRunner runner;
    private int depth;

    public void translate(Pipeline pipeline) {
        pipeline.traverseTopologically(this);
    }

    public CompositeBehavior enterCompositeTransform(Node node) {
        LOG.info("{} enterCompositeTransform- {}", genSpaces(this.depth), node.getFullName());
        ++this.depth;
        PTransform<?, ?> transform = node.getTransform();
        if (transform != null) {
            System.out.println(transform.toString());
        }
        return CompositeBehavior.ENTER_TRANSFORM;
    }

    public void leaveCompositeTransform(Node node) {
        --this.depth;
        LOG.info("{} leaveCompositeTransform- {}", genSpaces(this.depth), node.getFullName());
    }

    public void visitPrimitiveTransform(Node node) {
        LOG.info("{} visitPrimitiveTransform- {}", genSpaces(this.depth), node.getFullName());
        PTransform<?,?> transform = node.getTransform();
        System.out.println(transform.toString());
    }

    protected static String genSpaces(int n) {
        StringBuilder builder = new StringBuilder();

        for(int i = 0; i < n; ++i) {
            builder.append("|   ");
        }

        return builder.toString();
    }



}
