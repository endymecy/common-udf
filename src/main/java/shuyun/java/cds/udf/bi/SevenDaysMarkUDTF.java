package shuyun.java.cds.udf.bi;

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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * Created by endy on 2015/10/10.
 *
 * 返回窗口为7天的集合
 */

@Description(name = "seven_days_mark",
        value = "_FUNC_(a) -seven day mark")
public class SevenDaysMarkUDTF extends GenericUDTF{
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
            String input = objects[0].toString();

            for(int i = 0; i < 7; ++i) {
                try {
                    this.forward(new String[]{this.addDay(input, i)});
                } catch (Exception var5) {
                    ;
                }
            }
        }
    }

    @Override
    public void close() throws HiveException {

    }


    public String addDay(String time, int days) throws ParseException {
        String add = null;
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        Date timeNow = df.parse(time);
        Calendar begin = Calendar.getInstance();
        begin.setTime(timeNow);
        begin.add(5, days);
        add = df.format(begin.getTime());
        return add;
    }
}
