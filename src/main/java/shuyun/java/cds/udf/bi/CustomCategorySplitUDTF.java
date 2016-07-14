package shuyun.java.cds.udf.bi;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
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
        name = "custom_category_split",
        value = "_FUNC_(arg1)"
)
public class CustomCategorySplitUDTF extends GenericUDTF{
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        List inputFields = argOIs.getAllStructFieldRefs();
        ObjectInspector[] udtfInputOIs = new ObjectInspector[inputFields.size()];

        for(int i = 0; i < inputFields.size(); ++i) {
            udtfInputOIs[i] = ((StructField)inputFields.get(i)).getFieldObjectInspector();
        }

        if(udtfInputOIs.length != 1) {
            throw new UDFArgumentLengthException("ExplodeMap takes only one argument");
        } else if(udtfInputOIs[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentException("ExplodeMap takes string as a parameter");
        } else {
            ArrayList fieldNames = new ArrayList();
            ArrayList fieldOIs = new ArrayList();
            fieldNames.add("flag");
            fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
            return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
        }
    }

    @Override
    public void process(Object[] objects) throws HiveException {
        if(objects[0] != null) {
            String[] input = objects[0].toString().split(",");

            for(int i = 0; i < input.length; ++i) {
                try {
                    if(StringUtils.isNotEmpty(input[i])) {
                        this.forward(new String[]{input[i]});
                    } else {
                        this.forward(new String[]{""});
                    }
                } catch (Exception var5) {
                    ;
                }
            }
        } else {
            this.forward(new String[]{""});
        }
    }

    @Override
    public void close() throws HiveException {

    }
}
