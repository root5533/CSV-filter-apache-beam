package org.wso2.beam.localrunner;

import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.AppliedPTransform;

import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;

public class RootProvider {

    PipelineOptions options;

    public RootProvider(PipelineOptions options) {
        this.options = options;
    }

    public Collection<?> getInitialInputs(AppliedPTransform<?, ?, ?> transform, int targetParallelism) throws Exception {

        //Decide whether bounded or bounded
        Collection<?> input;
        if (transform.getTransform() instanceof Read.Bounded) {
            BoundedReadEvaluator eval = new BoundedReadEvaluator(this.options);
            eval.getInitialInputs(transform, targetParallelism);
//            ConcurrentLinkedQueue queue = new ConcurrentLinkedQueue();
//            queue.po
        }

        return null;

    }

}
