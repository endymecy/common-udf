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
        name = "distribute_cumu_group",
        value = "_FUNC_(arg1,arg2)"
)
public class DistributeCumuGroupUDTF extends GenericUDTF{
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        List inputFields = argOIs.getAllStructFieldRefs();
        ObjectInspector[] udtfInputOIs = new ObjectInspector[inputFields.size()];

        for(int i = 0; i < inputFields.size(); ++i) {
            udtfInputOIs[i] = ((StructField)inputFields.get(i)).getFieldObjectInspector();
        }

        if(udtfInputOIs.length != 2) {
            throw new UDFArgumentException("DistributeInternal() takes  two argument");
        } else {
            ArrayList fieldNames = new ArrayList();
            ArrayList fieldOIs = new ArrayList();
            fieldNames.add("cumu_group");
            fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
            return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
        }
    }
    @Override
    public void process(Object[] objects) throws HiveException {
        String internalS = objects[0].toString();
        String groupS = objects[1].toString();
        int internalI = Integer.parseInt(internalS);
        int groupI = Integer.parseInt(groupS);
        String[] forwardObj = new String[1];
        int i;
        switch(internalI) {
            case 10:
                for(i = groupI; i < 42; ++i) {
                    forwardObj[0] = String.valueOf(i);
                    this.forward(forwardObj);
                }

                return;
            case 21:
                for(i = groupI; i < 22; ++i) {
                    forwardObj[0] = String.valueOf(i);
                    this.forward(forwardObj);
                }

                return;
            case 30:
                for(i = groupI; i < 28; ++i) {
                    forwardObj[0] = String.valueOf(i);
                    this.forward(forwardObj);
                }

                return;
            default:
                forwardObj[0] = String.valueOf(-1);
                this.forward(forwardObj);
        }
    }

    @Override
    public void close() throws HiveException {

    }
}
