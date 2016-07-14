package shuyun.java.cds.udf.collect;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;

import java.util.List;
import java.util.Map;

/**
 * Created by endy on 2015/10/12.
 * 组合两个list或者两个map
 */
@Description(name = "combine",
        value = "_FUNC_(a,b) - Returns a combined list of two lists, or a combined map of two maps "
)
public class Combine extends GenericUDF {
    private ObjectInspector.Category category;
    private StandardListObjectInspector stdListInspector;
    private StandardMapObjectInspector stdMapInspector;
    private ListObjectInspector[] listInspectorList;
    private MapObjectInspector[] mapInspectorList;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length < 2) {
            throw new UDFArgumentException("Usage: combine takes 2 or more maps or lists, and combines the result");
        }
        ObjectInspector first = objectInspectors[0];
        this.category = first.getCategory();

        if (category == ObjectInspector.Category.LIST) {
            this.listInspectorList = new ListObjectInspector[objectInspectors.length];
            this.listInspectorList[0] = (ListObjectInspector) first;
            for (int i = 1; i < objectInspectors.length; ++i) {
                ObjectInspector argInsp = objectInspectors[i];
                if (!ObjectInspectorUtils.compareTypes(first, argInsp)) {
                    throw new UDFArgumentException("Combine must either be all maps or all lists of the same type");
                }
                this.listInspectorList[i] = (ListObjectInspector) argInsp;
            }
            this.stdListInspector = (StandardListObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(first, ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA);
            return stdListInspector;
        } else if (category == ObjectInspector.Category.MAP) {
            this.mapInspectorList = new MapObjectInspector[objectInspectors.length];
            this.mapInspectorList[0] = (MapObjectInspector) first;
            for (int i = 1; i < objectInspectors.length; ++i) {
                ObjectInspector argInsp = objectInspectors[i];
                if (!ObjectInspectorUtils.compareTypes(first, argInsp)) {
                    throw new UDFArgumentException("Combine must either be all maps or all lists of the same type");
                }
                this.mapInspectorList[i] = (MapObjectInspector) argInsp;
            }
            this.stdMapInspector = (StandardMapObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(first, ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA);
            return stdMapInspector;
        } else {
            throw new UDFArgumentException(" combine only takes maps or lists.");
        }
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        if (category == ObjectInspector.Category.LIST) {
            int currSize = 0;
            Object theList = stdListInspector.create(currSize);
            int lastIdx = 0;
            for (int i = 0; i < deferredObjects.length; ++i) {
                List addList = listInspectorList[i].getList(deferredObjects[i].get());
                currSize += addList.size();
                stdListInspector.resize(theList, currSize);

                for (int j = 0; j < addList.size(); ++j) {
                    Object uninspObj = addList.get(j);
                    Object stdObj = ObjectInspectorUtils.copyToStandardObject(uninspObj, listInspectorList[i].getListElementObjectInspector(), ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA);
                    stdListInspector.set(theList, lastIdx + j, stdObj);
                }
                lastIdx += addList.size();
            }
            return theList;
        } else if (category == ObjectInspector.Category.MAP) {
            Object theMap = stdMapInspector.create();
            for (int i = 0; i < deferredObjects.length; ++i) {
                if (deferredObjects[i].get() != null) {
                    Map addMap = mapInspectorList[i].getMap(deferredObjects[i].get());
                    for (Object uninspObj : addMap.entrySet()) {
                        Map.Entry uninspEntry = (Map.Entry) uninspObj;
                        Object stdKey = ObjectInspectorUtils.copyToStandardObject(uninspEntry.getKey(), mapInspectorList[i].getMapKeyObjectInspector(), ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA);
                        Object stdVal = ObjectInspectorUtils.copyToStandardObject(uninspEntry.getValue(), mapInspectorList[i].getMapValueObjectInspector(), ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA);
                        stdMapInspector.put(theMap, stdKey, stdVal);
                    }
                }
            }
            return theMap;
        } else {
            throw new HiveException(" Only maps or lists are supported ");
        }
    }

    @Override
    public String getDisplayString(String[] strings) {
        StringBuilder sb = new StringBuilder("combine( ");
        for (int i = 0; i < strings.length - 1; ++i) {
            sb.append(strings[i]);
            sb.append(",");
        }
        sb.append(strings[strings.length - 1]);
        sb.append(")");
        return sb.toString();
    }
}
