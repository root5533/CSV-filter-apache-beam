package org.wso2.beam.localrunner;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.WriteFiles;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.WindowedValue;

import java.util.Iterator;
import java.util.Map;

public class TransformExecutor {

//    private Map<?, ?> inputBundle;
//    DirectGraph graph;
//    private Map<?, ?> resultBundle;
//
//    private TransformExecutor(DirectGraph graph, Map<?, ?> inputBundle) {
//        this.graph = graph;
//        this.inputBundle = inputBundle;
//    }
//
//    public static TransformExecutor create(DirectGraph graph, Map<?, ?> inputBundle) {
//        return new TransformExecutor(graph, inputBundle);
//    }
//
//    public void run() {
//        //read source
//        if (inputBundle != null) {
//
//            BoundedReadEvaluator evaluator = BoundedReadEvaluator.getBoundedReadEvaluator();
//            for (Iterator iter = inputBundle.keySet().iterator(); iter.hasNext();) {
//                evaluator.processElement((WindowedValue) iter.next());
//            }
//
//
//
//        }
//    }

    private AppliedPTransform currentTransform;
    private Bundle bundle;

    public TransformExecutor(AppliedPTransform transform, Bundle bundle) {
        this.currentTransform = transform;
        this.bundle = bundle;
    }

    public boolean run() {
        try {
            if (this.currentTransform.getTransform() instanceof Read.Bounded) {
                ReadEvaluator evaluator = new ReadEvaluator(this.currentTransform);
                SourceWrapper reader = evaluator.createSourceWrapper();
                this.bundle.setSourceReader(reader, this.currentTransform.getOutputs());
            } else if (this.currentTransform.getTransform() instanceof ParDo.MultiOutput) {
                //Implement simplerunnerdofn for all pardo

            } else if (this.currentTransform.getTransform() instanceof WriteFiles) {
                //implement right file
            } else {
                System.err.println("Not a valid transform that can be executed in Siddhi");
                return false;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }

}
