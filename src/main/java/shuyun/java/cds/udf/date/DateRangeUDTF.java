package shuyun.java.cds.udf.date;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by endy on 2015/10/10.
 * 输入起始时间、结束时间以及步长，得到时间和序号集合
 *
 * select id, rng.date
 * from tab1
 * lateral view( date_range( tab1.start_date, tab1.end_date ) ) rng as date, index
 */
@Description(name = "date_range",
        value = "_FUNC_(a,b,c) - Generates a range of integers from a to b incremented by c"
                + " or the elements of a map into multiple rows and columns ")
public class DateRangeUDTF extends GenericUDTF {
    private static DateTimeFormatter YYMMDD = DateTimeFormat.forPattern("YYYYMMdd");
    private StringObjectInspector startInspector = null;
    private StringObjectInspector endInspector = null;
    private IntObjectInspector incrInspector = null;

    private final Object[] forwardListObj = new Object[2];
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs)
            throws UDFArgumentException {

        List inputFields = argOIs.getAllStructFieldRefs();
        ObjectInspector[] udtfInputOIs = new ObjectInspector[inputFields.size()];

        for(int i = 0; i < inputFields.size(); ++i) {
            udtfInputOIs[i] = ((StructField)inputFields.get(i)).getFieldObjectInspector();
        }

        if (udtfInputOIs.length < 2 || udtfInputOIs.length > 3) {
            throw new UDFArgumentException("DateRange takes <startdate>, <enddate>, <optional increment>");
        }

        if (!(udtfInputOIs[0] instanceof StringObjectInspector))
            throw new UDFArgumentException("DateRange takes <startdate>, <enddate>, <optional increment>");
        else
            startInspector = (StringObjectInspector) udtfInputOIs[0];

        if (!(udtfInputOIs[1] instanceof StringObjectInspector))
            throw new UDFArgumentException("DateRange takes <startdate>, <enddate>, <optional increment>");
        else
            endInspector = (StringObjectInspector) udtfInputOIs[1];

        if (udtfInputOIs.length == 3) {
            if (!(udtfInputOIs[2] instanceof IntObjectInspector))
                throw new UDFArgumentException("DateRange takes <startdate>, <enddate>, <optional increment>");
            else
                incrInspector = (IntObjectInspector) udtfInputOIs[2];
        }


        ArrayList<String> fieldNames = new ArrayList<String>();
        fieldNames.add("date");
        fieldNames.add("index");
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,
                fieldOIs);
    }


    @Override
    public void process(Object[] objects) throws HiveException {
        String start = null;
        String end = null;
        int incr = 1;
        switch (objects.length) {
            case 3:
                incr = incrInspector.get(objects[2]);
            case 2:
                start = startInspector.getPrimitiveJavaObject(objects[0]);
                end = endInspector.getPrimitiveJavaObject(objects[1]);
                break;
        }

        try {
            DateTime startDt = YYMMDD.parseDateTime(start);
            DateTime endDt = YYMMDD.parseDateTime(end);
            int i = 0;
            for (DateTime dt = startDt; dt.isBefore(endDt) || dt.isEqual(endDt); dt = dt.plusDays(incr), i++) {
                forwardListObj[0] = YYMMDD.print(dt);
                forwardListObj[1] = new Integer(i);

                forward(forwardListObj);
            }
        } catch (IllegalArgumentException badFormat) {
            throw new HiveException("Unable to parse dates; start = " + start + " ; end = " + end);
        }
    }

    @Override
    public void close() throws HiveException {

    }
}
