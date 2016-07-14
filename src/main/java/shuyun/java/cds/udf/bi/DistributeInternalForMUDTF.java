package shuyun.java.cds.udf.bi;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by endy on 2015/10/12.
 */
@Description(
        name = "distribute_internal_for_m",
        value = "_FUNC_(arg1,arg2)"
)
public class DistributeInternalForMUDTF extends GenericUDTF{
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        List inputFields = argOIs.getAllStructFieldRefs();
        ObjectInspector[] udtfInputOIs = new ObjectInspector[inputFields.size()];

        for(int i = 0; i < inputFields.size(); ++i) {
            udtfInputOIs[i] = ((StructField)inputFields.get(i)).getFieldObjectInspector();
        }

        if(udtfInputOIs.length != 2) {
            throw new UDFArgumentException("mDistributeInternal() takes only two argument");
        } else {
            ArrayList fieldNames = new ArrayList();
            ArrayList fieldOIs = new ArrayList();
            fieldNames.add("internal");
            fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
            fieldNames.add("norm_type");
            fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
            fieldNames.add("group_monetary");
            fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
            return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
        }
    }
    @Override
    public void process(Object[] objects) throws HiveException {
        String frequencyS = objects[0].toString();
        String paymentS = objects[1].toString();
        double paymentD = Double.parseDouble(paymentS);
        int frequencyI = Integer.parseInt(frequencyS);
        double averagePay = paymentD / (double)frequencyI;
        int[] internalNorm = new int[]{10, 20, 30, 50, 100, 500, 1000, 2000};
        String[] forwardObj = new String[3];

        for(int i = 0; i < internalNorm.length; ++i) {
            int groupMonetary = (int)Math.max(1.0D, Math.ceil(paymentD / (double)internalNorm[i]));
            if(groupMonetary > 20) {
                forwardObj[0] = String.valueOf(internalNorm[i]);
                forwardObj[1] = "all";
                forwardObj[2] = "21";
                this.forward(forwardObj);
            } else {
                forwardObj[0] = String.valueOf(internalNorm[i]);
                forwardObj[1] = "all";
                forwardObj[2] = String.valueOf(groupMonetary);
                this.forward(forwardObj);
            }

            int everGroMonetary = (int)Math.max(1.0D, Math.ceil(averagePay / (double)internalNorm[i]));
            if(everGroMonetary > 20) {
                forwardObj[0] = String.valueOf(internalNorm[i]);
                forwardObj[1] = "everage";
                forwardObj[2] = "21";
                this.forward(forwardObj);
            } else {
                forwardObj[0] = String.valueOf(internalNorm[i]);
                forwardObj[1] = "everage";
                forwardObj[2] = String.valueOf(everGroMonetary);
                this.forward(forwardObj);
            }
        }
    }

    @Override
    public void close() throws HiveException {

    }
}
