package shuyun.java.cds.udf.timeserials;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;

import java.util.Map;

/**
 * Created by endy on 2015/10/13.
 */
@Description(
        name = "vector_normalize",
        value = " Normalize a Vector"
)
public class VectorNormalize extends GenericUDF{
    private ListObjectInspector listInspector;
    private MapObjectInspector mapInspector;
    private PrimitiveObjectInspector valueInspector;

    private StandardListObjectInspector retListInspector;
    private StandardMapObjectInspector retMapInspector;

    public Object evaluateList(Object listObj) {
        Object retList = retListInspector.create(0);
        double tot = 0.0;
        for (int i = 0; i < listInspector.getListLength(listObj); ++i) {
            Object listVal = this.listInspector.getListElement(listObj, i);
            double listDbl = NumericUtil.getNumericValue(valueInspector, listVal);
            tot += listDbl;
        }
        for (int i = 0; i < listInspector.getListLength(listObj); ++i) {
            Object listVal = this.listInspector.getListElement(listObj, i);
            double listDbl = NumericUtil.getNumericValue(valueInspector, listVal);
            retListInspector.set(retList, i, NumericUtil.castToPrimitiveNumeric(listDbl / tot,
                    ((PrimitiveObjectInspector) retListInspector.getListElementObjectInspector()).getPrimitiveCategory()));
        }

        return retList;
    }

    public Object evaluateMap(Object uninspMapObj) {
        Object retMap = retMapInspector.create();
        Map map = mapInspector.getMap(uninspMapObj);
        double tot = 0.0;
        for (Object mapKey : map.keySet()) {
            Object mapValObj = map.get(mapKey);
            double mapValDbl = NumericUtil.getNumericValue(valueInspector, mapValObj);
            tot += mapValDbl;
        }
        for (Object mapKey : map.keySet()) {
            Object mapValObj = map.get(mapKey);
            double mapValDbl = NumericUtil.getNumericValue(valueInspector, mapValObj);
            double newVal = mapValDbl / tot;

            Object stdKey = ObjectInspectorUtils.copyToStandardJavaObject(mapKey,
                    mapInspector.getMapKeyObjectInspector());
            Object stdVal = NumericUtil.castToPrimitiveNumeric(newVal,
                    ((PrimitiveObjectInspector) retMapInspector.getMapValueObjectInspector()).getPrimitiveCategory());
            retMapInspector.put(retMap, stdKey, stdVal);

        }
        return retMap;
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length != 1)
            usage("Must have one argument.");

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

        if (listInspector != null) {
            retListInspector = ObjectInspectorFactory.getStandardListObjectInspector(
                    ObjectInspectorUtils.getStandardObjectInspector(valueInspector));
            return retListInspector;
        } else {
            retMapInspector = ObjectInspectorFactory.getStandardMapObjectInspector(
                    ObjectInspectorUtils.getStandardObjectInspector(mapInspector.getMapKeyObjectInspector(),
                            ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA),
                    ObjectInspectorUtils.getStandardObjectInspector(valueInspector,
                            ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA));
            return retMapInspector;
        }
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
        return "vector_normalize";
    }


    private void usage(String message) throws UDFArgumentException {
        throw new UDFArgumentException("vector_normalize: Normalize a vector : " + message);
    }
}
