package org.wso2.beam.localrunner;

import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

public class ReadEvaluator<T> {

    AppliedPTransform<PBegin, PCollection<T>, PTransform<PBegin, PCollection<T>>> transform;

    public ReadEvaluator(AppliedPTransform<PBegin, PCollection<T>, PTransform<PBegin, PCollection<T>>> transform) {
        this.transform = transform;
    }

    public SourceWrapper createSourceWrapper() throws Exception {
        Read.Bounded boundedInput = (Read.Bounded) this.transform.getTransform();
        BoundedSource<T> source = boundedInput.getSource();
        SourceWrapper sourceWrapper = new SourceWrapper(source, 1, transform.getPipeline().getOptions());
        return sourceWrapper;
    }

}
