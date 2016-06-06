package org.carbondata.spark.load;

import java.util.ArrayList;
import java.util.Arrays;
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

    TableBlockInfo block1 =
        new TableBlockInfo("path1", 123, "1", new String[] { "1", "2", "3" }, 111);
    TableBlockInfo block2 =
        new TableBlockInfo("path2", 123, "2", new String[] { "2", "3", "4" }, 111);
    TableBlockInfo block3 =
        new TableBlockInfo("path3", 123, "3", new String[] { "3", "4", "1" }, 111);
    TableBlockInfo block4 =
        new TableBlockInfo("path4", 123, "4", new String[] { "1", "2", "4" }, 111);

    inputMap.put(block1, Arrays.asList(new String[]{"1","2","3"}));
    inputMap.put(block2, Arrays.asList(new String[]{"2","3","4"}));
    inputMap.put(block3, Arrays.asList(new String[]{"3","4","1"}));
    inputMap.put(block4, Arrays.asList(new String[]{"1","2","4"}));

    List<TableBlockInfo> inputBlocks = new ArrayList(6);
    inputBlocks.add(block1);
    inputBlocks.add(block2);
    inputBlocks.add(block3);
    inputBlocks.add(block4);

    Map<String, List<TableBlockInfo>> outputMap
        = CarbonLoaderUtil.nodeBlockMapping(inputBlocks, 4);

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
    if ((notInNodeLocality / numberOfBlocks) * 100 > 30) {
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
        new TableBlockInfo("part-0-0-1462341987000", 123, "1", new String[] { "1", "2", "3" }, 111);
    TableBlockInfo block2 =
        new TableBlockInfo("part-1-0-1462341987000", 123, "2", new String[] { "1", "2", "3" }, 111);
    TableBlockInfo block3 =
        new TableBlockInfo("part-2-0-1462341987000", 123, "3", new String[] { "1", "2", "3" }, 111);
    TableBlockInfo block4 =
        new TableBlockInfo("part-3-0-1462341987000", 123, "4", new String[] { "1", "2", "3" }, 111);
    TableBlockInfo block5 =
        new TableBlockInfo("part-4-0-1462341987000", 123, "5", new String[] { "1", "2", "3" }, 111);

    inputMap.put(block1, Arrays.asList(new String[]{"1","2","3"}));
    inputMap.put(block2, Arrays.asList(new String[]{"1","2","3"}));
    inputMap.put(block3, Arrays.asList(new String[]{"1","2","3"}));
    inputMap.put(block4, Arrays.asList(new String[]{"1","2","3"}));
    inputMap.put(block5, Arrays.asList(new String[]{"1","2","3"}));

    List<TableBlockInfo> inputBlocks = new ArrayList(6);
    inputBlocks.add(block1);
    inputBlocks.add(block2);
    inputBlocks.add(block3);
    inputBlocks.add(block4);
    inputBlocks.add(block5);

    Map<String, List<TableBlockInfo>> outputMap = CarbonLoaderUtil.nodeBlockMapping(inputBlocks, 3);

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
        new TableBlockInfo("part-0-0-1462341987000", 123, "1", new String[] { "1", "2", "3" }, 111);
    TableBlockInfo block2 =
        new TableBlockInfo("part-1-0-1462341987000", 123, "2", new String[] { "1", "2", "3" }, 111);
    TableBlockInfo block3 =
        new TableBlockInfo("part-2-0-1462341987000", 123, "3", new String[] { "1", "2", "3" }, 111);
    TableBlockInfo block4 =
        new TableBlockInfo("part-3-0-1462341987000", 123, "4", new String[] { "1", "2", "3" }, 111);
    TableBlockInfo block5 =
        new TableBlockInfo("part-4-0-1462341987000", 123, "5", new String[] { "1", "2", "3" }, 111);
    TableBlockInfo block6 =
        new TableBlockInfo("part-5-0-1462341987000", 123, "6", new String[] { "1", "2", "3" }, 111);

    inputMap.put(block1, Arrays.asList(new String[]{"1","2","3"}));
    inputMap.put(block2, Arrays.asList(new String[]{"1","2","3"}));
    inputMap.put(block3, Arrays.asList(new String[]{"1","2","3"}));
    inputMap.put(block4, Arrays.asList(new String[]{"1","2","3"}));
    inputMap.put(block5, Arrays.asList(new String[]{"1","2","3"}));
    inputMap.put(block6, Arrays.asList(new String[]{"1","2","3"}));


    List<TableBlockInfo> inputBlocks = new ArrayList(6);
    inputBlocks.add(block1);
    inputBlocks.add(block2);
    inputBlocks.add(block3);
    inputBlocks.add(block4);
    inputBlocks.add(block5);
    inputBlocks.add(block6);

    Map<String, List<TableBlockInfo>> outputMap = CarbonLoaderUtil.nodeBlockMapping(inputBlocks, 4);

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
        new TableBlockInfo("part-1-0-1462341987000", 123, "1", new String[] { "2", "4" }, 111);
    TableBlockInfo block2 =
        new TableBlockInfo("part-2-0-1462341987000", 123, "2", new String[] { "2", "4" }, 111);
    TableBlockInfo block3 =
        new TableBlockInfo("part-3-0-1462341987000", 123, "3", new String[] { "2", "4" }, 111);
    TableBlockInfo block4 =
        new TableBlockInfo("part-4-0-1462341987000", 123, "4", new String[] { "2", "4" }, 111);
    TableBlockInfo block5 =
        new TableBlockInfo("part-5-0-1462341987000", 123, "5", new String[] { "2", "4" }, 111);
    TableBlockInfo block6 =
        new TableBlockInfo("part-6-0-1462341987000", 123, "6", new String[] { "2", "4" }, 111);
    TableBlockInfo block7 =
        new TableBlockInfo("part-7-0-1462341987000", 123, "7", new String[] { "3", "4" }, 111);
    TableBlockInfo block8 =
        new TableBlockInfo("part-8-0-1462341987000", 123, "8", new String[] { "3", "4" }, 111);
    TableBlockInfo block9 =
        new TableBlockInfo("part-9-0-1462341987000", 123, "9", new String[] { "3", "4" }, 111);
    TableBlockInfo block10 =
        new TableBlockInfo("part-10-0-1462341987000", 123, "9", new String[] { "1", "4" }, 111);

    inputMap.put(block1, Arrays.asList(new String[]{"2","4"}));
    inputMap.put(block2, Arrays.asList(new String[]{"2","4"}));
    inputMap.put(block3, Arrays.asList(new String[]{"2","4"}));
    inputMap.put(block4, Arrays.asList(new String[]{"2","4"}));
    inputMap.put(block5, Arrays.asList(new String[]{"2","4"}));
    inputMap.put(block6, Arrays.asList(new String[]{"2","4"}));
    inputMap.put(block7, Arrays.asList(new String[]{"3","4"}));
    inputMap.put(block8, Arrays.asList(new String[]{"3","4"}));
    inputMap.put(block9, Arrays.asList(new String[]{"3","4"}));
    inputMap.put(block10, Arrays.asList(new String[]{"1","4"}));

    List<TableBlockInfo> inputBlocks = new ArrayList(6);
    inputBlocks.add(block1);
    inputBlocks.add(block2);
    inputBlocks.add(block3);
    inputBlocks.add(block4);
    inputBlocks.add(block5);
    inputBlocks.add(block6);
    inputBlocks.add(block7);
    inputBlocks.add(block8);
    inputBlocks.add(block9);
    inputBlocks.add(block10);

    Map<String, List<TableBlockInfo>> outputMap = CarbonLoaderUtil.nodeBlockMapping(inputBlocks, 4);

    Assert.assertTrue(calculateBlockDistribution(inputMap, outputMap, 10, 4));

    Assert.assertTrue(calculateBlockLocality(inputMap, outputMap, 10, 4));
  }

}