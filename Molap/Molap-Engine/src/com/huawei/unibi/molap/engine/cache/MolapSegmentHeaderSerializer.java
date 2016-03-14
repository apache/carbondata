/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwdh/HjOjN0Brs7b7TRorj6S6iAIeaqK90lj7BAM
GSGxBgQesNQqo6KC4ec0RAhqTDySy3IgDQuAqg0UcRggCkym/I7w+0iWRnW1juA5e2Hs9qop
hPatpCs2yRgURXdpOnSdlijLSaCzpFxdJs4l78XS5Rbvs0YwZS7mQVp8mSmE4w==*/
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
public class MolapSegmentHeaderSerializer implements Serializable
{

    /**
     * 
     */
    private static final long serialVersionUID = 2958864791162240071L;

    public MolapSegmentHeader deserialize(DataInput in) throws IOException, ClassNotFoundException
    {
        int len = in.readInt();
        byte[] bytes = new byte[len];
        in.readFully(bytes);
        ByteArrayInputStream arrayInputStream = new ByteArrayInputStream(bytes);
        ObjectInputStream stream = new ObjectInputStream(arrayInputStream);
        MolapSegmentHeader header = (MolapSegmentHeader)stream.readObject();
        return header;
    }

    public void serialize(DataOutput out, MolapSegmentHeader header) throws IOException
    {
        ByteArrayOutputStream arrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream stream = new ObjectOutputStream(arrayOutputStream);
        stream.writeObject(header);
        byte[] byteArray = arrayOutputStream.toByteArray();
        out.writeInt(byteArray.length);
        out.write(byteArray);

    }
   
}
