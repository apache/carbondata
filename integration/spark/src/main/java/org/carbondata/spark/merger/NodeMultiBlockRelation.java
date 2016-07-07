package org.carbondata.spark.merger;

import java.util.List;

import org.carbondata.core.carbon.datastore.block.Distributable;

public class NodeMultiBlockRelation implements Comparable<NodeMultiBlockRelation> {

  private final List<Distributable> blocks;
  private final String node;

  public NodeMultiBlockRelation(String node, List<Distributable> blocks) {
    this.node = node;
    this.blocks = blocks;

  }

  public List<Distributable> getBlocks() {
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
