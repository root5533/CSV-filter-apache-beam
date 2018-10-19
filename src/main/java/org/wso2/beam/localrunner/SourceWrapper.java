package org.wso2.beam.localrunner;

import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;

import java.util.List;

public class SourceWrapper<T> {

    List<? extends BoundedSource<T>> splitSources;

    public SourceWrapper(BoundedSource source, int parallelism, PipelineOptions options) throws Exception {
        long estimatedBytes = source.getEstimatedSizeBytes(options);
        splitSources = source.split(estimatedBytes, options);
    }

}
