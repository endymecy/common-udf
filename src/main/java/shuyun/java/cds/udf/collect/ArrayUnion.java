package shuyun.java.cds.udf.collect;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Created by endy on 2015/10/12.
 * 返回多个数组的union
 */
@Description(name = "array_union",
        value = "_FUNC_(array1, array2, ...) - Returns the union of a set of arrays "
)
public class ArrayUnion extends GenericUDF{
    private StandardListObjectInspector retValInspector;
    private ListObjectInspector[] listInspectorArr;

    private class InspectableObject implements Comparable {
        public Object o;
        public ObjectInspector oi;

        public InspectableObject(Object o, ObjectInspector oi) {
            this.o = o;
            this.oi = oi;
        }

        @Override
        public int hashCode() {
            return ObjectInspectorUtils.hashCode(o, oi);
        }

        @Override
        public int compareTo(Object arg0) {
            InspectableObject otherInsp = (InspectableObject) arg0;
            return ObjectInspectorUtils.compare(o, oi, otherInsp.o, otherInsp.oi);
        }

        @Override
        public boolean equals(Object other) {
            return compareTo(other) == 0;
        }
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length < 2) {
            throw new UDFArgumentException(" Expecting at least two arrays as arguments ");
        }
        ObjectInspector first = objectInspectors[0];
        listInspectorArr = new ListObjectInspector[objectInspectors.length];
        if (first.getCategory() == ObjectInspector.Category.LIST) {
            listInspectorArr[0] = (ListObjectInspector) first;
        } else {
            throw new UDFArgumentException(" Expecting an array as first argument ");
        }
        for (int i = 1; i < objectInspectors.length; ++i) {
            if (objectInspectors[i].getCategory() != ObjectInspector.Category.LIST) {
                throw new UDFArgumentException(" Expecting arrays arguments ");
            }
            ListObjectInspector checkInspector = (ListObjectInspector) objectInspectors[i];
            if (!ObjectInspectorUtils.compareTypes(listInspectorArr[0].getListElementObjectInspector(), checkInspector.getListElementObjectInspector())) {
                throw new UDFArgumentException(" Array types must match " + listInspectorArr[0].getTypeName() + " != " + checkInspector.getTypeName());
            }
            listInspectorArr[i] = checkInspector;
        }

        retValInspector = (StandardListObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(first);
        return retValInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        Set<InspectableObject> objects = new TreeSet<InspectableObject>();

        for (int i = 0; i < deferredObjects.length; ++i) {
            Object undeferred = deferredObjects[i].get();
            for (int j = 0; j < listInspectorArr[i].getListLength(undeferred); ++j) {
                Object nonStd = listInspectorArr[i].getListElement(undeferred, j);
                InspectableObject stdInsp = new InspectableObject(nonStd, listInspectorArr[i].getListElementObjectInspector());
                objects.add(stdInsp);
            }
        }

        List retVal = (List) retValInspector.create(0);
        for (Object io : objects) {
            InspectableObject inspObj = (InspectableObject) io;
            Object stdObj = ObjectInspectorUtils.copyToStandardObject(inspObj.o, inspObj.oi);
            retVal.add(stdObj);
        }
        return retVal;
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "array_union(" + strings[0] + ", " + strings[1] + " )";
    }
}
