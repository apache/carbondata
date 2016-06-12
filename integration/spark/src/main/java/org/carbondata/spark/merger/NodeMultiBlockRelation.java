package org.carbondata.spark.merger;

import java.util.List;

import org.carbondata.core.carbon.datastore.block.TableBlockInfo;

public class NodeMultiBlockRelation implements Comparable<NodeMultiBlockRelation> {

  private final List<TableBlockInfo> blocks;
  private final String node;

  public NodeMultiBlockRelation(String node, List<TableBlockInfo> blocks) {
    this.node = node;
    this.blocks = blocks;

  }

  public List<TableBlockInfo> getBlocks() {
    return blocks;
  }

  public String getNode() {
    return node;
  }

  @Override public int compareTo(NodeMultiBlockRelation obj) {
    return this.blocks.size() - obj.getBlocks().size();
  }

  @Override public boolean equals(Object obj) {
    if (!(obj instanceof NodeMultiBlockRelation)) {
      return false;
    }
    NodeMultiBlockRelation o = (NodeMultiBlockRelation) obj;
    return blocks.equals(o.blocks) && node.equals(o.node);
  }

  @Override public int hashCode() {
    return blocks.hashCode() + node.hashCode();
  }
}
