/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwdh/HjOjN0Brs7b7TRorj6S6iAIeaqK90lj7BAM
GSGxBvcB+GsV6Te0Jm+oRSjSZg4OvnfAILXqQ0ELLwySF/V6gEo0NijYaqXEAt6lsgxtUQwG
iBgakp8N0kpdYGjcYvht2WU5po548Zk1V+78e4kEl8MFeEPjBbc85urjj2Msag==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
package com.huawei.unibi.molap.engine.cache;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * Class responsible to serialize and deserialize from and to JDBM DB
 * @author A00902732
 *
 */
public class MolapSegmentBodySerializer implements  Serializable
{
    
    /**
     * 
     */
    private static final long serialVersionUID = -8170501658357083083L;


    public void serialize(DataOutput out, MolapSegmentBody body) throws IOException
    {
        ByteArrayOutputStream arrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream stream = new ObjectOutputStream(arrayOutputStream);
        stream.writeObject(body);
        byte[] byteArray = arrayOutputStream.toByteArray();
        out.writeInt(byteArray.length);
        out.write(byteArray);
    }


    public MolapSegmentBody deserialize(DataInput in) throws IOException, ClassNotFoundException
    {
       int len = in.readInt();
       byte[] bytes = new byte[len];
       in.readFully(bytes);
       ByteArrayInputStream arrayInputStream = new ByteArrayInputStream(bytes);
       ObjectInputStream stream = new ObjectInputStream(arrayInputStream);
       MolapSegmentBody body = (MolapSegmentBody)stream.readObject();
       return body;
    }
}
