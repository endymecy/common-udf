package shuyun.java.cds.udf.timeserials;

/**
 * Created by endy on 2015/10/13.
 */

import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;

public final class NumericUtil {

    public static boolean isNumericCategory(PrimitiveObjectInspector.PrimitiveCategory cat) {
        switch (cat) {
            case DOUBLE:
            case FLOAT:
            case LONG:
            case INT:
            case SHORT:
            case BYTE:
                return true;
            default:
                return false;
        }
    }

    /**
     * Cast the output of a Numeric ObjectInspector
     * to a double
     *
     * @param objInsp
     * @return
     */
    public static double getNumericValue(PrimitiveObjectInspector objInsp, Object val) {
        switch (objInsp.getPrimitiveCategory()) {
            case DOUBLE:
                return ((DoubleObjectInspector) objInsp).get(val);
            case FLOAT:
            case LONG:
            case INT:
            case SHORT:
            case BYTE:
                Number num = (Number) objInsp.getPrimitiveJavaObject(val);
                return num.doubleValue();
            default:
                return 0.0;
        }
    }


    /**
     * Cast a double to an object required by the ObjectInspector
     * associated with the given PrimitiveCategory
     *
     * @param val
     * @param cat
     * @return
     */
    public static Object castToPrimitiveNumeric(double val, PrimitiveObjectInspector.PrimitiveCategory cat) {
        switch (cat) {
            case DOUBLE:
                return new Double(val);
            case FLOAT:
                return new Float((float) val);
            case LONG:
                return new Long((long) val);
            case INT:
                return new Integer((int) val);
            case SHORT:
                return new Short((short) val);
            case BYTE:
                return new Byte((byte) val);
            default:
                return null;
        }
    }

}

