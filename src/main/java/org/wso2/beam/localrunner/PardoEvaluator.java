package org.wso2.beam.localrunner;

import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.values.PCollection;

public class PardoEvaluator<InputT> {

    AppliedPTransform pardo;
    CommittedBundle<InputT> bundle;

    public PardoEvaluator(AppliedPTransform transform, CommittedBundle<InputT> bundle) {
        this.pardo = transform;
        this.bundle = bundle;
    }

    public void execute() throws Exception {



    }

}
