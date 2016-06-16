package org.carbondata.core.carbon.datastore;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.carbondata.core.carbon.CarbonTableIdentifier;
import org.carbondata.core.carbon.datastore.block.AbstractIndex;
import org.carbondata.core.carbon.datastore.block.TableBlockInfo;
import org.carbondata.core.carbon.datastore.exception.IndexBuilderException;

import junit.framework.TestCase;
import org.junit.BeforeClass;
import org.junit.Test;

public class SegmentTaskIndexStoreTest extends TestCase {

  private SegmentTaskIndexStore indexStore;

  @BeforeClass public void setUp() {
    indexStore = SegmentTaskIndexStore.getInstance();
  }

  @Test public void testloadAndGetTaskIdToSegmentsMapForSingleSegment() throws IOException {
    String canonicalPath =
        new File(this.getClass().getResource("/").getPath() + "/../../").getCanonicalPath();
    File file = new File(canonicalPath + "/src/test/resources/part-0-0-1466029397000.carbondata");
    TableBlockInfo info =
        new TableBlockInfo(file.getAbsolutePath(), 0, "0", new String[] { "loclhost" },
            file.length());
    CarbonTableIdentifier carbonTableIdentifier = new CarbonTableIdentifier("default", "t3", "1");
    AbsoluteTableIdentifier absoluteTableIdentifier =
        new AbsoluteTableIdentifier("/src/test/resources", carbonTableIdentifier);
    Map<String, List<TableBlockInfo>> mapOfSegmentToTableBlockInfoList = new HashMap<>();
    mapOfSegmentToTableBlockInfoList.put("0", Arrays.asList(new TableBlockInfo[] { info }));
    Map<String, AbstractIndex> loadAndGetTaskIdToSegmentsMap = null;
    try {
      loadAndGetTaskIdToSegmentsMap = indexStore
          .loadAndGetTaskIdToSegmentsMap(mapOfSegmentToTableBlockInfoList, absoluteTableIdentifier);
    } catch (IndexBuilderException e) {
      assertTrue(false);
    }
    assertTrue(loadAndGetTaskIdToSegmentsMap.size() == 1);
    indexStore.removeTableBlocks(Arrays.asList(new String[] { "0" }), absoluteTableIdentifier);
  }

  //@Test
  public void testloadAndGetTaskIdToSegmentsMapForSameSegmentLoadedConcurrently()
      throws IOException {
    String canonicalPath =
        new File(this.getClass().getResource("/").getPath() + "/../../").getCanonicalPath();
    File file = new File(canonicalPath + "/src/test/resources/part-0-0-1466029397000.carbondata");
    TableBlockInfo info =
        new TableBlockInfo(file.getAbsolutePath(), 0, "0", new String[] { "loclhost" },
            file.length());
    CarbonTableIdentifier carbonTableIdentifier = new CarbonTableIdentifier("default", "t3", "1");
    AbsoluteTableIdentifier absoluteTableIdentifier =
        new AbsoluteTableIdentifier("/src/test/resources", carbonTableIdentifier);
    Map<String, List<TableBlockInfo>> mapOfSegmentToTableBlockInfoList = new HashMap<>();
    mapOfSegmentToTableBlockInfoList.put("0", Arrays.asList(new TableBlockInfo[] { info }));
    ExecutorService executor = Executors.newFixedThreadPool(2);

    executor
        .submit(new SegmentLoaderThread(mapOfSegmentToTableBlockInfoList, absoluteTableIdentifier));
    executor
        .submit(new SegmentLoaderThread(mapOfSegmentToTableBlockInfoList, absoluteTableIdentifier));
    executor.shutdown();
    try {
      executor.awaitTermination(1, TimeUnit.DAYS);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    assertTrue(indexStore.getSegmentBTreeIfExists(absoluteTableIdentifier, "0").size() == 1);
    indexStore.removeTableBlocks(Arrays.asList(new String[] { "0" }), absoluteTableIdentifier);
  }

  @Test public void testloadAndGetTaskIdToSegmentsMapForDifferentSegmentLoadedConcurrently()
      throws IOException {
    String canonicalPath =
        new File(this.getClass().getResource("/").getPath() + "/../../").getCanonicalPath();
    File file = new File(canonicalPath + "/src/test/resources/part-0-0-1466029397000.carbondata");
    TableBlockInfo info =
        new TableBlockInfo(file.getAbsolutePath(), 0, "0", new String[] { "loclhost" },
            file.length());
    TableBlockInfo info1 =
        new TableBlockInfo(file.getAbsolutePath(), 0, "1", new String[] { "loclhost" },
            file.length());
    CarbonTableIdentifier carbonTableIdentifier = new CarbonTableIdentifier("default", "t3", "1");
    AbsoluteTableIdentifier absoluteTableIdentifier =
        new AbsoluteTableIdentifier("/src/test/resources", carbonTableIdentifier);
    Map<String, List<TableBlockInfo>> mapOfSegmentToTableBlockInfoList = new HashMap<>();
    Map<String, List<TableBlockInfo>> mapOfSegmentToTableBlockInfoList1 = new HashMap<>();
    mapOfSegmentToTableBlockInfoList.put("0", Arrays.asList(new TableBlockInfo[] { info }));
    mapOfSegmentToTableBlockInfoList1.put("1", Arrays.asList(new TableBlockInfo[] { info1 }));
    ExecutorService executor = Executors.newFixedThreadPool(2);

    executor
        .submit(new SegmentLoaderThread(mapOfSegmentToTableBlockInfoList, absoluteTableIdentifier));
    executor.submit(
        new SegmentLoaderThread(mapOfSegmentToTableBlockInfoList1, absoluteTableIdentifier));

    executor.shutdown();
    try {
      executor.awaitTermination(1, TimeUnit.DAYS);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    assertTrue(indexStore.getSegmentBTreeIfExists(absoluteTableIdentifier, "0").size() == 1);
    assertTrue(indexStore.getSegmentBTreeIfExists(absoluteTableIdentifier, "1").size() == 1);
    indexStore.removeTableBlocks(Arrays.asList(new String[] { "0" }), absoluteTableIdentifier);
    indexStore.removeTableBlocks(Arrays.asList(new String[] { "1" }), absoluteTableIdentifier);
  }

  private class SegmentLoaderThread implements Callable<Void> {
    private Map<String, List<TableBlockInfo>> mapOfSegmentToTableBlockInfoList;

    private AbsoluteTableIdentifier absoluteTableIdentifier;

    public SegmentLoaderThread(Map<String, List<TableBlockInfo>> mapOfSegmentToTableBlockInfoList,
        AbsoluteTableIdentifier absoluteTableIdentifier) {
      // TODO Auto-generated constructor stub
      this.mapOfSegmentToTableBlockInfoList = mapOfSegmentToTableBlockInfoList;
      this.absoluteTableIdentifier = absoluteTableIdentifier;
    }

    @Override public Void call() throws Exception {
      indexStore
          .loadAndGetTaskIdToSegmentsMap(mapOfSegmentToTableBlockInfoList, absoluteTableIdentifier);
      return null;
    }

  }
}
