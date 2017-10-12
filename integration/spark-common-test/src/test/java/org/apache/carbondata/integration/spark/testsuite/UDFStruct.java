package org.apache.carbondata.integration.spark.testsuite;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import java.util.Arrays;

public class UDFStruct extends GenericUDF {
    StandardStructObjectInspector structInputObjectInspector;

    public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        structInputObjectInspector = (StandardStructObjectInspector) args[0];
        return structInputObjectInspector;
    }

    public Object evaluate(GenericUDF.DeferredObject[] args) throws HiveException {
        if (args.length != 1) return null;
        //UDF actual Logic here. append 'UDF' to each value
        Object object = args[0].get();

        if (object == null) return null;

        if ((structInputObjectInspector.getAllStructFieldRefs().size()) > 0) {
            return Arrays.asList(structInputObjectInspector.getStructFieldsDataAsList(object).get(0) + "-UDF",
                    structInputObjectInspector.getStructFieldsDataAsList(object).get(1) + "-UDF");
        } else {
            return null;
        }
    }

    @Override
    public String getDisplayString(String[] args) {
        return "";
    }
}
