package org.wso2.beam.localrunner;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import org.apache.beam.runners.core.construction.TransformInputs;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.PValue;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.*;

public class LocalGraphVisitor extends Pipeline.PipelineVisitor.Defaults {

    private static final Logger LOG = LoggerFactory.getLogger(LocalGraphVisitor.class);
    private Map<AppliedPTransform<?, ?, ?>, String> stepNames = new HashMap();
    private Set<AppliedPTransform<?, ?, ?>> rootTransforms = new HashSet();
    private Queue<AppliedPTransform<?, ?, ?>> parentTransforms = new LinkedList<>();
    private ListMultimap<PInput, AppliedPTransform<?, ?, ?>> perElementConsumers = ArrayListMultimap.create();
    private ListMultimap<PValue, AppliedPTransform<?, ?, ?>> allConsumers = ArrayListMultimap.create();
    private Map<PCollection<?>, AppliedPTransform<?, ?, ?>> producers = new HashMap();
    private int numTransforms = 0;
    private int depth;

    public CompositeBehavior enterCompositeTransform(Node node) {
        ++this.depth;
        LOG.info("{} enterCompositeTransform- {}", genSpaces(this.depth), node.getFullName());
        if (node.getEnclosingNode() != null) {
            if (node.getEnclosingNode().isRootNode()) {
                AppliedPTransform<?, ?, ?> appliedPTransform = this.getAppliedTransform(node);
                this.parentTransforms.add(appliedPTransform);
            }
        }
        return CompositeBehavior.ENTER_TRANSFORM;
    }

    public void leaveCompositeTransform(Node node) {
        --this.depth;
        LOG.info("{} leaveCompositeTransform- {}", genSpaces(this.depth), node.getFullName());
    }

    public void visitPrimitiveTransform(Node node) {
        LOG.info("{} visitPrimitiveTransform- {}", genSpaces(this.depth), node.getFullName());

        AppliedPTransform<?, ?, ?> appliedPTransform = this.getAppliedTransform(node);
        this.stepNames.put(appliedPTransform, this.genStepName());
        if (node.getInputs().isEmpty()) {
            this.rootTransforms.add(appliedPTransform);
        } else {
            Collection<PValue> mainInputs = TransformInputs.nonAdditionalInputs(node.toAppliedPTransform(this.getPipeline()));
            if (!mainInputs.containsAll(node.getInputs().values())) {
                LOG.info("Inputs reduced to {} from {} by removing additional inputs", mainInputs, node.getInputs().values());
            }
            Iterator iter = mainInputs.iterator();
            PValue value;
            while(iter.hasNext()) {
                value = (PValue) iter.next();
                this.perElementConsumers.put(value, appliedPTransform);
            }
            iter = node.getInputs().values().iterator();
            while(iter.hasNext()) {
                value = (PValue) iter.next();
                this.allConsumers.put(value, appliedPTransform);
            }
        }
    }

    public void visitValue(PValue value, Node node) {
        LOG.info("{} visitValue- {}", genSpaces(this.depth), node.getFullName());
        AppliedPTransform<?, ?, ?> appliedTransform = this.getAppliedTransform(node);
        if (value instanceof PCollection && !this.producers.containsKey(value)) {
            this.producers.put((PCollection)value, appliedTransform);
        }
    }

    private AppliedPTransform<?, ?, ?> getAppliedTransform(Node node) {
        LOG.info("{} getAppliedTransform- {}", genSpaces(this.depth), node.getFullName());
        return node.toAppliedPTransform(this.getPipeline());
    }

    public DirectGraph getGraph() {
        LOG.info("{} getGraph- {}", genSpaces(this.depth));
        return DirectGraph.create(producers, perElementConsumers, rootTransforms, parentTransforms, stepNames);
    }

    protected static String genSpaces(int n) {
        StringBuilder builder = new StringBuilder();

        for(int i = 0; i < n; ++i) {
            builder.append("|   ");
        }

        return builder.toString();
    }

    private String genStepName() {
        return String.format("s%s", this.numTransforms++);
    }


}
