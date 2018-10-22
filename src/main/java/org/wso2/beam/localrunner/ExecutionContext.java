package org.wso2.beam.localrunner;

import org.apache.beam.sdk.values.PCollection;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class ExecutionContext {

    private final DirectGraph graph;
    private HashMap<PCollection, CommittedBundle> bundles = new HashMap<>();
    private HashMap<PCollection, CommittedBundle> rootBundles = new HashMap<>();
    private Iterator rootBundlesIterator;

    public ExecutionContext(DirectGraph graph) {
        this.graph = graph;
    }

    public void addRootBundle(CommittedBundle bundle) {
        this.bundles.put(bundle.getPCollection(), bundle);
        this.rootBundles.put(bundle.getPCollection(), bundle);
    }

    public CommittedBundle getBundle(PCollection key) {
        return this.bundles.get(key);
    }

    public CommittedBundle getPendingRootBundle() {
        if (this.rootBundlesIterator == null) {
            this.rootBundlesIterator = this.rootBundles.values().iterator();
        }
        return getRootBundle();
    }

    private CommittedBundle getRootBundle() {
        return (CommittedBundle) this.rootBundlesIterator.next();
    }

}
