package org.wso2.beam.localrunner;

import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

import java.util.Collection;
import java.util.List;

public class BoundedReadEvaluator<T> {

    private final PipelineOptions options;

    public BoundedReadEvaluator(PipelineOptions options) {
        this.options = options;
    }

    public Collection<?> getInitialInputs(AppliedPTransform<PBegin, PCollection<T>, PTransform<PBegin, PCollection<T>>> transform, int targetParallelism) throws Exception {
        Read.Bounded boundedInput = (Read.Bounded) transform.getTransform();
        BoundedSource<T> source = boundedInput.getSource();
        long estimatedBytes = source.getEstimatedSizeBytes(this.options);
        long bytesPerBundle = estimatedBytes / (long)targetParallelism;
        List<? extends BoundedSource<T>> bundles = source.split(bytesPerBundle, this.options);
        System.out.println(bundles.size());
        return null;
    }

}
