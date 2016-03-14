package com.huawei.unibi.molap.engine.expression;

public enum DataType {
    StringType(0), DateType(1), TimestampType(2), BooleanType(1), IntegerType(3), FloatType(4), LongType(5), DoubleType(6), NullType(7), ArrayType(8), StructType(9);
    private int presedenceOrder;
    public int getPresedenceOrder()
    {
        return presedenceOrder;
    }
    private DataType(int value)
    {
        this.presedenceOrder = value;
    }
}
