package org.wso2.beam.localrunner;

public class EvaluationContext {

    private DirectGraph graph;

    public static EvaluationContext create(DirectGraph graph) {
        return new EvaluationContext(graph);
    }

    private EvaluationContext(DirectGraph graph) {
        this.graph = graph;
    }

}
