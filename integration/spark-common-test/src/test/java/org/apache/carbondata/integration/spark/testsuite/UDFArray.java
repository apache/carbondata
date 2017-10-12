package org.apache.carbondata.integration.spark.testsuite;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

public class UDFArray extends GenericUDF {
    ListObjectInspector listInputObjectInspector;

    public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {

        listInputObjectInspector = (ListObjectInspector) args[0];
        return listInputObjectInspector.getListElementObjectInspector();
    }

    public Object evaluate(DeferredObject[] args) throws HiveException {
        if (args.length != 1) return null;
        //UDF actual Logic here. append 'UDF' to each value

        Object object = args[0].get();

        if (object == null) return null;

        if ((listInputObjectInspector.getListLength(object)) > 0) {
            return listInputObjectInspector.getListElement(object, 0) + "-UDF";
        } else {
            return null;
        }
    }

    @Override
    public String getDisplayString(String[] args) {
        return "";
    }
}