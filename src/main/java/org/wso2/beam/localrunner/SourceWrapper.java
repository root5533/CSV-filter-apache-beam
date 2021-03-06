package org.wso2.beam.localrunner;

import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.util.WindowedValue;

import java.util.ArrayList;
import java.util.List;

public class SourceWrapper<OutputT> {

    List<? extends BoundedSource<OutputT>> splitSources;
    private boolean isOpen = false;
    PipelineOptions options;
    List<BoundedSource<OutputT>> localSplitSources;
    List<BoundedReader<OutputT>> localReaders;

    public SourceWrapper(BoundedSource source, int parallelism, PipelineOptions options) throws Exception {
        this.options = options;
        long estimatedBytes = source.getEstimatedSizeBytes(options);
        splitSources = source.split(estimatedBytes, options);
        this.localSplitSources = new ArrayList<>();
        this.localReaders = new ArrayList<>();
    }

    public boolean isOpen() {
        return this.isOpen;
    }

    public void open() throws Exception {
        this.isOpen = true;
        for ( int i = 0; i < this.splitSources.size(); i++ ) {
            BoundedSource<OutputT> source = (BoundedSource) this.splitSources.get(i);
            BoundedReader<OutputT> reader = source.createReader(this.options);
            this.localSplitSources.add(source);
            this.localReaders.add(reader);
        }
    }

    public void run(DoFnOperator op) throws Exception {

        /**
         *Run the source to emit each element to DoFnOperator delegate
         */
        if (this.localReaders.size() == 1) {
            BoundedReader<OutputT> reader = localReaders.get(0);
            boolean hasData = reader.start();
            if (hasData) {
                this.emitElement(op, reader);
            }
            hasData = reader.advance();
            while (hasData) {
                this.emitElement(op, reader);
                hasData = reader.advance();
            }
        }

    }

    private void emitElement(DoFnOperator op, BoundedReader reader) {
        WindowedValue elem = WindowedValue.timestampedValueInGlobalWindow(reader.getCurrent(), reader.getCurrentTimestamp());
        op.processElement(elem);
    }

}
