package org.wso2.beam.localrunner;

import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.util.WindowedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;

public class ExecutorService {

    private static final Logger LOG = LoggerFactory.getLogger(ExecutorService.class);
    private final EvaluationContext context;
    private final int targetParallelism;

    public static ExecutorService create(int targetParallelism, EvaluationContext context) {
        return new ExecutorService(targetParallelism, context);
    }

    private ExecutorService(int targetParallelism, EvaluationContext context) {
        this.targetParallelism = targetParallelism;
        this.context = context;
    }

    public void start(DirectGraph graph, RootProvider rootProvider) {

        int numOfSplits = Math.max(3, this.targetParallelism);
        TransformExecutor executor;
        AppliedPTransform root;
        for (Iterator iter = graph.getRootTransforms().iterator(); iter.hasNext(); ) {
            root = (AppliedPTransform)iter.next();
            try {
                Map<WindowedValue, AppliedPTransform<?, ?, ?>> initialInputs = rootProvider.getInitialInputs(root, numOfSplits);
                if (initialInputs != null) {
                    executor = TransformExecutor.create(graph, initialInputs);
                    executor.run();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

}
