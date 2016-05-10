package org.carbondata.integration.spark.load;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.carbondata.core.carbon.datastore.block.TableBlockInfo;

import org.junit.Test;
import org.pentaho.di.core.util.Assert;

/**
 * Test class for LoaderUtil Test
 */
public class CarbonLoaderUtilTest {

  /**
   * Test case with 4 blocks and 4 nodes with 3 replication.
   *
   * @throws Exception
   */
  @Test public void nodeBlockMapping() throws Exception {

    Map<TableBlockInfo, List<String>> inputMap = new HashMap<TableBlockInfo, List<String>>(5);

    TableBlockInfo block1 = new TableBlockInfo("path1", 123, 1, new String[] { "sdf" }, 111);
    TableBlockInfo block2 = new TableBlockInfo("path2", 123, 2, new String[] { "sdf" }, 111);
    TableBlockInfo block3 = new TableBlockInfo("path3", 123, 3, new String[] { "sdf" }, 111);
    TableBlockInfo block4 = new TableBlockInfo("path4", 123, 4, new String[] { "sdf" }, 111);

    List<String> list1 = new ArrayList(3);
    list1.add("1");
    list1.add("2");
    list1.add("3");
    List<String> list2 = new ArrayList(3);
    list2.add("2");
    list2.add("3");
    list2.add("4");
    List<String> list3 = new ArrayList(3);
    list3.add("3");
    list3.add("4");
    list3.add("1");
    List<String> list4 = new ArrayList(3);
    list4.add("1");
    list4.add("2");
    list4.add("4");

    inputMap.put(block1, list1);
    inputMap.put(block2, list2);
    inputMap.put(block3, list3);
    inputMap.put(block4, list4);

    Map<String, List<TableBlockInfo>> outputMap = CarbonLoaderUtil.nodeBlockMapping(inputMap, 4, 4);

    Assert.assertTrue(calculateBlockDistribution(inputMap, outputMap, 4, 4));

    Assert.assertTrue(calculateBlockLocality(inputMap, outputMap, 4, 4));
  }

  private boolean calculateBlockLocality(Map<TableBlockInfo, List<String>> inputMap,
      Map<String, List<TableBlockInfo>> outputMap, int numberOfBlocks, int numberOfNodes) {

    double notInNodeLocality = 0;
    for (Map.Entry<String, List<TableBlockInfo>> entry : outputMap.entrySet()) {

      List<TableBlockInfo> blockListOfANode = entry.getValue();

      for (TableBlockInfo eachBlock : blockListOfANode) {

        // for each block check the node locality

        List<String> blockLocality = inputMap.get(eachBlock);
        if (!blockLocality.contains(entry.getKey())) {
          notInNodeLocality++;
        }
      }
    }

    System.out.println(
        ((notInNodeLocality / numberOfBlocks) * 100) + " " + "is the node locality mismatch");
    if ((notInNodeLocality / numberOfBlocks) * 100 > 25) {
      return false;
    }
    return true;
  }

  private boolean calculateBlockDistribution(Map<TableBlockInfo, List<String>> inputMap,
      Map<String, List<TableBlockInfo>> outputMap, int numberOfBlocks, int numberOfNodes) {

    int nodesPerBlock = numberOfBlocks / numberOfNodes;

    for (Map.Entry<String, List<TableBlockInfo>> entry : outputMap.entrySet()) {

      if (entry.getValue().size() < nodesPerBlock) {
        return false;
      }
    }
    return true;
  }

  /**
   * Test case with 5 blocks and 3 nodes
   *
   * @throws Exception
   */
  @Test public void nodeBlockMappingTestWith5blocks3nodes() throws Exception {

    Map<TableBlockInfo, List<String>> inputMap = new HashMap<TableBlockInfo, List<String>>(5);

    TableBlockInfo block1 =
        new TableBlockInfo("part-0-0-1462341987000", 123, 1, new String[] { "sdf" }, 111);
    TableBlockInfo block2 =
        new TableBlockInfo("part-1-0-1462341987000", 123, 2, new String[] { "sdf" }, 111);
    TableBlockInfo block3 =
        new TableBlockInfo("part-2-0-1462341987000", 123, 3, new String[] { "sdf" }, 111);
    TableBlockInfo block4 =
        new TableBlockInfo("part-3-0-1462341987000", 123, 4, new String[] { "sdf" }, 111);
    TableBlockInfo block5 =
        new TableBlockInfo("part-4-0-1462341987000", 123, 5, new String[] { "sdf" }, 111);

    List<String> list1 = new ArrayList(3);
    list1.add("1");
    list1.add("2");
    list1.add("3");

    inputMap.put(block1, list1);
    inputMap.put(block2, list1);
    inputMap.put(block3, list1);
    inputMap.put(block4, list1);
    inputMap.put(block5, list1);

    Map<String, List<TableBlockInfo>> outputMap = CarbonLoaderUtil.nodeBlockMapping(inputMap, 5, 3);

    Assert.assertTrue(calculateBlockDistribution(inputMap, outputMap, 5, 3));

    Assert.assertTrue(calculateBlockLocality(inputMap, outputMap, 5, 3));

  }

  /**
   * Test case with 6 blocks and 4 nodes where 4 th node doesnt have any local data.
   *
   * @throws Exception
   */
  @Test public void nodeBlockMappingTestWith6Blocks4nodes() throws Exception {

    Map<TableBlockInfo, List<String>> inputMap = new HashMap<TableBlockInfo, List<String>>(5);

    TableBlockInfo block1 =
        new TableBlockInfo("part-0-0-1462341987000", 123, 1, new String[] { "sdf" }, 111);
    TableBlockInfo block2 =
        new TableBlockInfo("part-1-0-1462341987000", 123, 2, new String[] { "sdf" }, 111);
    TableBlockInfo block3 =
        new TableBlockInfo("part-2-0-1462341987000", 123, 3, new String[] { "sdf" }, 111);
    TableBlockInfo block4 =
        new TableBlockInfo("part-3-0-1462341987000", 123, 4, new String[] { "sdf" }, 111);
    TableBlockInfo block5 =
        new TableBlockInfo("part-4-0-1462341987000", 123, 4, new String[] { "sdf" }, 111);
    TableBlockInfo block6 =
        new TableBlockInfo("part-5-0-1462341987000", 123, 4, new String[] { "sdf" }, 111);

    List<String> list1 = new ArrayList(3);
    list1.add("1");
    list1.add("2");
    list1.add("3");
    List<String> list2 = new ArrayList(3);
    list2.add("1");
    list2.add("2");
    list2.add("3");
    List<String> list3 = new ArrayList(3);
    list3.add("1");
    list3.add("2");
    list3.add("3");
    List<String> list4 = new ArrayList(3);
    list4.add("1");
    list4.add("2");
    list4.add("3");
    List<String> list5 = new ArrayList(3);
    list5.add("1");
    list5.add("2");
    list5.add("3");
    List<String> list6 = new ArrayList(3);
    list6.add("1");
    list6.add("2");
    list6.add("3");

    inputMap.put(block1, list1);
    inputMap.put(block2, list2);
    inputMap.put(block3, list3);
    inputMap.put(block4, list4);
    inputMap.put(block5, list5);
    inputMap.put(block6, list6);

    Map<String, List<TableBlockInfo>> outputMap = CarbonLoaderUtil.nodeBlockMapping(inputMap, 6, 4);

    Assert.assertTrue(calculateBlockDistribution(inputMap, outputMap, 6, 4));

    Assert.assertTrue(calculateBlockLocality(inputMap, outputMap, 6, 4));

  }

  /**
   * Test case with 10 blocks and 4 nodes with 10,60,30 % distribution
   *
   * @throws Exception
   */
  @Test public void nodeBlockMappingTestWith10Blocks4nodes() throws Exception {

    Map<TableBlockInfo, List<String>> inputMap = new HashMap<TableBlockInfo, List<String>>(5);

    TableBlockInfo block1 =
        new TableBlockInfo("part-1-0-1462341987000", 123, 1, new String[] { "sdf" }, 111);
    TableBlockInfo block2 =
        new TableBlockInfo("part-2-0-1462341987000", 123, 2, new String[] { "sdf" }, 111);
    TableBlockInfo block3 =
        new TableBlockInfo("part-3-0-1462341987000", 123, 3, new String[] { "sdf" }, 111);
    TableBlockInfo block4 =
        new TableBlockInfo("part-4-0-1462341987000", 123, 4, new String[] { "sdf" }, 111);
    TableBlockInfo block5 =
        new TableBlockInfo("part-5-0-1462341987000", 123, 4, new String[] { "sdf" }, 111);
    TableBlockInfo block6 =
        new TableBlockInfo("part-6-0-1462341987000", 123, 4, new String[] { "sdf" }, 111);
    TableBlockInfo block7 =
        new TableBlockInfo("part-7-0-1462341987000", 123, 4, new String[] { "sdf" }, 111);
    TableBlockInfo block8 =
        new TableBlockInfo("part-8-0-1462341987000", 123, 4, new String[] { "sdf" }, 111);
    TableBlockInfo block9 =
        new TableBlockInfo("part-9-0-1462341987000", 123, 4, new String[] { "sdf" }, 111);
    TableBlockInfo block10 =
        new TableBlockInfo("part-10-0-1462341987000", 123, 4, new String[] { "sdf" }, 111);

    List<String> list1 = new ArrayList<String>(3);
    list1.add("2");
    list1.add("4");
    List<String> list2 = new ArrayList<String>(3);
    list2.add("2");
    list2.add("4");
    List<String> list3 = new ArrayList<String>(3);
    list3.add("2");
    list3.add("4");
    List<String> list4 = new ArrayList<String>(3);
    list4.add("2");
    list4.add("4");
    List<String> list5 = new ArrayList<String>(3);
    list5.add("2");
    list5.add("4");
    List<String> list6 = new ArrayList<String>(3);
    list6.add("2");
    list6.add("4");
    List<String> list7 = new ArrayList<String>(3);
    list7.add("3");
    list7.add("4");
    List<String> list8 = new ArrayList<String>(3);
    list8.add("3");
    list8.add("4");
    List<String> list9 = new ArrayList<String>(3);
    list9.add("3");
    list9.add("4");
    List<String> list10 = new ArrayList<String>(3);
    list10.add("1");
    list10.add("4");

    inputMap.put(block1, list1);
    inputMap.put(block2, list2);
    inputMap.put(block3, list3);
    inputMap.put(block4, list4);
    inputMap.put(block5, list5);
    inputMap.put(block6, list6);
    inputMap.put(block7, list7);
    inputMap.put(block8, list8);
    inputMap.put(block9, list9);
    inputMap.put(block10, list10);

    Map<String, List<TableBlockInfo>> outputMap =
        CarbonLoaderUtil.nodeBlockMapping(inputMap, 10, 4);

    Assert.assertTrue(calculateBlockDistribution(inputMap, outputMap, 10, 4));

    Assert.assertTrue(calculateBlockLocality(inputMap, outputMap, 10, 4));
  }

}