package org.apache.carbondata.examples.rtree;

import com.vividsolutions.jts.index.strtree.STRtree;

import java.io.Serializable;

public class RTreeIndexDetails implements Serializable {
    private static final long serialVersionUID = -4022024835627190048L;

    private STRtree rtree;

    private String filePath;
    private Integer BlockletId;

}
