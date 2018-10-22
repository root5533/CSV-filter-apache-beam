package org.wso2.beam.localrunner;

import org.apache.beam.sdk.runners.AppliedPTransform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;

/**
 * Hard coded execution of {@link org.wso2.beam.SimpleLocalRunner}
 */

public class SimpleLocalRunnerService {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleLocalRunnerService.class);
    private final int targetParallelism;

    public static SimpleLocalRunnerService create(int targetParallelism) {
        return new SimpleLocalRunnerService(targetParallelism);
    }

    private SimpleLocalRunnerService(int targetParallelism) {
        this.targetParallelism = targetParallelism;
    }

    public void start(DirectGraph graph, RootProvider rootProvider) {
        try {
            /**
             * Execute ReadFile/Read to generate split sources
             */
            ExecutionContext context = new ExecutionContext(graph);
            for (Iterator iter = graph.getRootTransforms().iterator(); iter.hasNext(); ) {
                AppliedPTransform root = (AppliedPTransform) iter.next();
                if (root.getFullName().equals("Readfile/Read")) {
                    ReadEvaluator evaluator = new ReadEvaluator(root, context);
                    evaluator.execute();
                }
            }

            /**
             * Execute ParDoTransform
             */
            CommittedBundle<SourceWrapper> rootBundle = context.getPendingRootBundle();
            List<AppliedPTransform<?, ?, ?>> transforms = graph.getPerElementConsumers(rootBundle.getPCollection());
            AppliedPTransform<?, ?, ?> pardoTransform = transforms.get(0);
            PardoEvaluator evaluator = new PardoEvaluator(pardoTransform, rootBundle, context);
            evaluator.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
