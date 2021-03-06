package org.wso2.beam.localrunner;

import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.runners.core.SimpleDoFnRunner;

public class PardoEvaluator<InputT> {

    AppliedPTransform pardo;
    CommittedBundle<InputT> bundle;
    ExecutionContext context;

    public PardoEvaluator(AppliedPTransform transform, CommittedBundle<InputT> bundle, ExecutionContext context) {
        this.pardo = transform;
        this.bundle = bundle;
        this.context = context;
    }

    public void execute() throws Exception {
        DoFnOperator operator = new DoFnOperator(this.pardo, this.context);
        operator.createRunner(this.bundle);
        SourceWrapper source = this.bundle.getSourceWrapper();
        source.open();
        source.run(operator);
        operator.finish();
    }

}
