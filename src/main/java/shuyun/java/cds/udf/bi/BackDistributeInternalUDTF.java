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
        name = "back_distribute_internal",
        value = "_FUNC_(arg1,arg2,arg3)"
)
public class BackDistributeInternalUDTF extends GenericUDTF{
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        List inputFields = argOIs.getAllStructFieldRefs();
        ObjectInspector[] udtfInputOIs = new ObjectInspector[inputFields.size()];

        for(int i = 0; i < inputFields.size(); ++i) {
            udtfInputOIs[i] = ((StructField)inputFields.get(i)).getFieldObjectInspector();
        }

        if(udtfInputOIs.length != 2) {
            throw new UDFArgumentException("backDistributeInternal() takes only two argument");
        } else {
            ArrayList fieldNames = new ArrayList();
            ArrayList fieldOIs = new ArrayList();
            fieldNames.add("internal");
            fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
            fieldNames.add("norm_type");
            fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
            fieldNames.add("group_buyback");
            fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
            return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
        }
    }

    @Override
    public void process(Object[] objects) throws HiveException {
        double firstInternal = Double.parseDouble(objects[0].toString());
        double allInternal = Double.parseDouble(objects[1].toString());
        String[] forwardObj = new String[3];
        int group;
        if(firstInternal <= 720.0D) {
            group = Math.max(1, (int)Math.ceil(firstInternal / 30.0D));
        } else if(firstInternal <= 1080.0D) {
            group = (int)Math.ceil((firstInternal - 720.0D) / 180.0D) + 24;
        } else {
            group = 27;
        }

        forwardObj[0] = "30";
        forwardObj[1] = "first";
        forwardObj[2] = String.valueOf(group);
        this.forward(forwardObj);
        if(allInternal <= 720.0D) {
            group = Math.max(1, (int)Math.ceil(allInternal / 30.0D));
        } else if(allInternal <= 1080.0D) {
            group = (int)Math.ceil((allInternal - 720.0D) / 180.0D) + 24;
        } else {
            group = 27;
        }

        forwardObj[0] = "30";
        forwardObj[1] = "all";
        forwardObj[2] = String.valueOf(group);
        this.forward(forwardObj);
        if(firstInternal <= 360.0D) {
            group = Math.max(1, (int)Math.ceil(firstInternal / 10.0D));
        } else if(firstInternal <= 1080.0D) {
            group = (int)Math.ceil((firstInternal - 360.0D) / 180.0D) + 36;
        } else {
            group = 41;
        }

        forwardObj[0] = "10";
        forwardObj[1] = "first";
        forwardObj[2] = String.valueOf(group);
        this.forward(forwardObj);
        if(allInternal <= 360.0D) {
            group = Math.max(1, (int)Math.ceil(allInternal / 10.0D));
        } else if(allInternal <= 1080.0D) {
            group = (int)Math.ceil((allInternal - 360.0D) / 180.0D) + 36;
        } else {
            group = 41;
        }

        forwardObj[0] = "10";
        forwardObj[1] = "all";
        forwardObj[2] = String.valueOf(group);
        this.forward(forwardObj);
    }

    @Override
    public void close() throws HiveException {

    }
}
