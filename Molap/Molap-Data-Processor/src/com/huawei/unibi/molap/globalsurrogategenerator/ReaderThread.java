package com.huawei.unibi.molap.globalsurrogategenerator;

import java.util.Map;
import java.util.concurrent.Callable;

import com.huawei.unibi.molap.datastorage.store.filesystem.MolapFile;

public class ReaderThread implements Callable<Map<String, Integer>>{
	 MolapFile file;

     public ReaderThread(MolapFile file)
     {
         this.file = file;
     }

     @Override
     public Map<String, Integer> call() throws Exception
     {
         return LevelGlobalSurrogateGeneratorThread.readLevelFileAndUpdateCache(file);
     }
}
