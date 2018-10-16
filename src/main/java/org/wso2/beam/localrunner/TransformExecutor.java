package org.wso2.beam.localrunner;

import org.apache.beam.sdk.util.WindowedValue;

import java.util.Iterator;
import java.util.Map;

public class TransformExecutor {

    private Map<?, ?> inputBundle;
    DirectGraph graph;
    private Map<?, ?> resultBundle;

    private TransformExecutor(DirectGraph graph, Map<?, ?> inputBundle) {
        this.graph = graph;
        this.inputBundle = inputBundle;
    }

    public static TransformExecutor create(DirectGraph graph, Map<?, ?> inputBundle) {
        return new TransformExecutor(graph, inputBundle);
    }

    public void run() {
        //read source
        if (inputBundle != null) {

            BoundedReadEvaluator evaluator = BoundedReadEvaluator.getBoundedReadEvaluator();
            for (Iterator iter = inputBundle.keySet().iterator(); iter.hasNext();) {
                evaluator.processElement((WindowedValue) iter.next());
            }

        }
    }

}
