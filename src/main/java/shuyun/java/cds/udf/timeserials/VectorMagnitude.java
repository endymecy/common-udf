package shuyun.java.cds.udf.timeserials;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.Map;

/**
 * Created by endy on 2015/10/13.
 * Magnitude of vector =   SQRT( Sum( val^2 ) )
 */
@Description(
        name = "vector_magnitude",
        value = " Magnitude of a vector."
)
public class VectorMagnitude extends GenericUDF{
    private ListObjectInspector listInspector;
    private MapObjectInspector mapInspector;
    private PrimitiveObjectInspector valueInspector;

    private StandardListObjectInspector retListInspector;
    private StandardMapObjectInspector retMapInspector;

    public Object evaluateList(Object listObj) {
        double sumSquares = 0.0;
        for (int i = 0; i < listInspector.getListLength(listObj); ++i) {
            Object listVal = this.listInspector.getListElement(listObj, i);
            double listDbl = NumericUtil.getNumericValue(valueInspector, listVal);
            sumSquares += listDbl * listDbl;
        }
        return Math.sqrt(sumSquares);
    }

    public Object evaluateMap(Object uninspMapObj) {
        Map map = mapInspector.getMap(uninspMapObj);
        double sumSquares = 0.0;
        for (Object mapKey : map.keySet()) {
            Object mapValObj = map.get(mapKey);
            double mapValDbl = NumericUtil.getNumericValue(valueInspector, mapValObj);

            sumSquares += mapValDbl * mapValDbl;
        }
        return Math.sqrt(sumSquares);
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length != 2)
            usage("Must have two arguments.");

        if (objectInspectors[0].getCategory() == ObjectInspector.Category.MAP) {
            this.mapInspector = (MapObjectInspector) objectInspectors[0];

            if (mapInspector.getMapKeyObjectInspector().getCategory() != ObjectInspector.Category.PRIMITIVE)
                usage("Vector map key must be a primitive");

            if (mapInspector.getMapValueObjectInspector().getCategory() != ObjectInspector.Category.PRIMITIVE)
                usage("Vector map value must be a primitive");

            this.valueInspector = (PrimitiveObjectInspector) mapInspector.getMapValueObjectInspector();
        } else if (objectInspectors[0].getCategory() == ObjectInspector.Category.LIST) {
            this.listInspector = (ListObjectInspector) objectInspectors[0];

            if (listInspector.getListElementObjectInspector().getCategory() != ObjectInspector.Category.PRIMITIVE)
                usage("Vector array value must be a primitive");

            this.valueInspector = (PrimitiveObjectInspector) listInspector.getListElementObjectInspector();
        } else {
            usage("First argument must be an array or map");
        }

        if (!NumericUtil.isNumericCategory(valueInspector.getPrimitiveCategory())) {
            usage(" Vector values must be numeric");
        }
        if (objectInspectors[1].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            usage(" scalar needs to be a primitive type.");
        }

        return PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arg0) throws HiveException {
        if (listInspector != null) {
            return evaluateList(arg0[0].get());
        } else {
            return evaluateMap(arg0[0].get());
        }
    }

    @Override
    public String getDisplayString(String[] arg0) {
        return "vector_scalar_mult";
    }


    private void usage(String message) throws UDFArgumentException {
        throw new UDFArgumentException("vector_scalar_mult: Multiply a vector times a scalar value : " + message);
    }
}
