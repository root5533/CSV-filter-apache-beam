package org.wso2.beam.localrunner;

import org.apache.beam.sdk.values.PCollection;

import java.util.LinkedList;
import java.util.Queue;

public class CommittedBundle<T> {

    PCollection bundle;
    Queue<T> values = new LinkedList<>();

    public CommittedBundle(PCollection bundle) {
        this.bundle = bundle;
    }

    public void addItem(T item) {
        values.add(item);
    }

    public PCollection getPCollection() {
        return this.bundle;
    }

    public SourceWrapper getSourceWrapper() {
        return (SourceWrapper) this.values.poll();
    }

}
