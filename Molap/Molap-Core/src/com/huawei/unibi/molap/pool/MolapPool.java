package com.huawei.unibi.molap.pool;

public interface MolapPool<E>
{
    E get();
    
    void put(E e);
    
}
