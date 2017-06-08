package org.apache.carbondata.hive.server;

/**
 * carbondata-parent
 *
 * @author guodong
 */
public class HiveEmbeddedMain
{
    public static void main(String[] args)
    {
        HiveEmbeddedServer2 server2 = new HiveEmbeddedServer2();
        try
        {
            server2.start();
            int port = server2.getFreePort();
            System.out.println("Hive embedded server started at port " + port);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
//        finally
//        {
//            server2.stop();
//        }
    }
}
