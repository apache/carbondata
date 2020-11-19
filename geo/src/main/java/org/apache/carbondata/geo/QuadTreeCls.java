/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.geo;

import java.awt.geom.Point2D;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.carbondata.common.logging.LogServiceFactory;

import org.apache.log4j.Logger;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

/**
 * Spatial region function processing related classes
 */
class GeometryOperation {
  private static final GeometryFactory geoFactory = new GeometryFactory();

  /**
   * Convert point object to polygon object in Geo
   *
   * @param polygon Area coordinates stored as a list
   * @return JTS Polygon objects
   */
  public static Polygon getPolygonByPointList(List<Point2D.Double> polygon) {
    int size = polygon.size();
    if (size < 3) {
      return null;
    } else {
      Coordinate[] rect = new Coordinate[size + 1];
      for (int i = 0; i < size; i++) {
        rect[i] = new Coordinate(polygon.get(i).x, polygon.get(i).y);
      }
      // making a closed polygon by keeping first point and last point same
      rect[size] = new Coordinate(polygon.get(0).x, polygon.get(0).y);
      return geoFactory.createPolygon(rect);
    }
  }

  /**
   * Convert point object to polygon object in Geo
   * @param polygon Area coordinates stored as a list
   * @return JTS Polygon objects
   */
  public static Polygon getPolygonByDoubleList(List<double[]> polygon) {
    int size = polygon.size();
    if (size < 4) {
      return null;
    } else {
      Coordinate[] rect = new Coordinate[size];
      for (int i = 0; i < size; i++) {
        rect[i] = new Coordinate(polygon.get(i)[0], polygon.get(i)[1]);
      }
      return geoFactory.createPolygon(rect);
    }
  }

  /**
   * Converting point objects to point objects in Geo
   * @param pointB Point2D Point object
   * @param isMaxDepth Whether is the last division
   * @param scale Set scale to point at last division
   * @return JTS Point object
   */
  public static Point getPointByPoint2D(Point2D.Double pointB, boolean isMaxDepth, int scale) {
    Coordinate point;
    if (isMaxDepth) {
      point = new Coordinate(
          new BigDecimal(pointB.x).setScale(scale, BigDecimal.ROUND_HALF_UP).doubleValue(),
          new BigDecimal(pointB.y).setScale(scale, BigDecimal.ROUND_HALF_UP).doubleValue());
    } else {
      point = new Coordinate(pointB.x, pointB.y);
    }
    return geoFactory.createPoint(point);
  }

  /**
   * Apart a and B do not intersect, a and B are polygons
   * @param polygonA polygon
   * @param polygonB polygon
   * @return true Polygons apart，false Polygons are inseparable
   */
  public static boolean disjoint(Geometry polygonA, List<Point2D.Double> polygonB) {
    Polygon polyB = getPolygonByPointList(polygonB);
    boolean result  = polygonA.disjoint(polyB);
    return result;
  }

  /**
   * A and B do not intersect each other, A is a polygon, B is a point
   * @param polygonA polygon
   * @param pointB point
   * @param isMaxDepth Whether is the last division
   * @param scale Set scale to point at last division
   * @return true Point away from polygon，false Points are inseparable from polygons
   */
  public static boolean disjoint(Geometry polygonA, Point2D.Double pointB,
      boolean isMaxDepth, int scale) {
    Point pointGeo = getPointByPoint2D(pointB, isMaxDepth, scale);
    boolean result = polygonA.disjoint(pointGeo);
    return result;
  }

  /**
   * contains - A contains B Compare polygon a with polygon B
   * @param polygonA  polygon
   * @param polygonB  polygon
   * @return 0 Polygon a contains polygon B (a = B or a > b),
   *        -1 Polygon a does not contain polygon B
   */
  public static boolean contains(Geometry polygonA, List<Point2D.Double> polygonB) {
    Polygon polyB = getPolygonByPointList(polygonB);
    return polygonA.contains(polyB);
  }


  /**
   * contains - A contains B Compare whether polygon a contains B
   * @param polygonA  polygon
   * @param pointB   point
   * @param isMaxDepth Whether is the last division
   * @param scale Set scale to point at last division
   * @return true Polygon a contains point B (B in a), false Polygon a does not contain point B
   */
  public static boolean contains(Geometry polygonA, Point2D.Double pointB,
      boolean isMaxDepth, int scale) {
    Point pointGeo = getPointByPoint2D(pointB, isMaxDepth, scale);
    boolean result = polygonA.contains(pointGeo);
    return result;
  }

  /**
   * intersect - A intersects B Indicates that polygon a intersects polygon B
   * @param polygonA polygon
   * @param polygonB polygon
   * @return true Polygon a intersects polygon B,false Polygon a does not intersect polygon B
   */
  public static boolean intersects(Geometry polygonA, List<Point2D.Double> polygonB) {
    Polygon polyB = getPolygonByPointList(polygonB);
    boolean result = polygonA.intersects(polyB);
    return result;
  }

  /**
   * intersect - A intersects B Represents the intersection of polygon A and point B
   * @param polygonA polygon
   * @param pointB point
   * @param isMaxDepth Whether is the last division
   * @param scale Set scale to point at last division
   * @return true Polygon a intersects point B,false Polygon a does not intersect point B
   */
  public static boolean intersects(Geometry polygonA, Point2D.Double pointB,
      boolean isMaxDepth, int scale) {
    Point pointGeo = getPointByPoint2D(pointB, isMaxDepth, scale);
    boolean result = polygonA.intersects(pointGeo);
    return result;
  }
}


/**
 * Polygon region object
 */
class QuadRect {
  public Double left;
  public Double top;
  public Double right;
  public Double bottom;
  public Point2D.Double topLeft;
  public Point2D.Double topRight;
  public Point2D.Double bottomRight;
  public Point2D.Double bottomLeft;

  /**
   * build func
   * @param topleft Left upper point
   * @param bottomRight Right lower point
   */
  public QuadRect(Point2D.Double topleft, Point2D.Double bottomRight) {
    this.left = topleft.x;
    this.top = topleft.y;
    this.right = bottomRight.x;
    this.bottom = bottomRight.y;
    this.topLeft = new Point2D.Double(this.left, this.top);
    this.topRight = new Point2D.Double(this.right, this.top);
    this.bottomRight = new Point2D.Double(this.right, this.bottom);
    this.bottomLeft = new Point2D.Double(this.left, this.bottom);
  }

  /**
   * build func
   * @param x Leftmost coordinates
   * @param y Bottom coordinate
   * @param x2 Rightmost coordinate
   * @param y2 Top coordinate
   */
  public QuadRect(double x, double y, double x2, double y2) {
    this.left = x;
    this.bottom = y;
    this.right = x2;
    this.top = y2;
    this.topLeft = new Point2D.Double(this.left, this.top);
    this.topRight = new Point2D.Double(this.right, this.top);
    this.bottomRight = new Point2D.Double(this.right, this.bottom);
    this.bottomLeft = new Point2D.Double(this.left, this.bottom);
  }

  /**
   * The given area is outside the current node area
   * @param polygonRect If the circumscribed rectangle of a given region is larger than
   *                    the coordinate with the node, it means it is outside the whole region
   * @return true The given area is outside the point,false The given area is within the point area
   */
  public boolean outsideBox(QuadRect polygonRect) {
    return polygonRect.left < this.left || polygonRect.right > this.right ||
               polygonRect.top > this.top || polygonRect.bottom < this.bottom;
  }

  /**
   * Get the polygon list of this area. The point is the rectangle of the inner center point of
   * the grid area center point. If it is the smallest grid, it is the coordinate of the
   * peripheral point
   * @return  List of vertices in rectangular area
   */
  public List<Point2D.Double> getPolygonPointList() {
    List<Point2D.Double> polygon = new ArrayList<>();
    polygon.add(this.topLeft);
    polygon.add(this.topRight);
    polygon.add(this.bottomRight);
    polygon.add(this.bottomLeft);
    return polygon;
  }

  /**
   * Get the coordinates of the center point of the area
   * @return Center point coordinates
   */
  public Double[] getMidelePoint() {
    double x = left + (right - left) / 2;
    double y = bottom + (top - bottom) / 2;
    return new Double [] {x, y};
  }

  /**
   * Get the center point of the grid
   * @return This grid represents the center point of the area
   */
  public Point2D.Double getMiddlePoint() {
    Double [] mPoint = getMidelePoint();
    return new Point2D.Double(mPoint[0], mPoint[1]);
  }

  /**
   * Split a region into four regions, reduce the creation of objects, and directly return
   * two coordinates of the region
   * @return Returns the coordinates of nine points cut into four areas, in the order of top left,
   * top middle, top right; middle left, middle right, bottom left, bottom middle, bottom right
   *
   */
  public List<Point2D.Double> getSplitRect() {
    Double [] mPoint = getMidelePoint();
    // The four remaining points, plus the four points of the boundary, constitute four regions
    // A region is divided into 4 sub regions by 9 points
    double middleTopX = mPoint[0];
    double middleTopY = this.top;
    Point2D.Double middleTop = new Point2D.Double(middleTopX, middleTopY);

    double middleBottomX = mPoint[0];
    double middleBottomY = this.bottom;
    Point2D.Double middleBottom = new Point2D.Double(middleBottomX, middleBottomY);

    double leftMiddleX = this.left;
    double leftMiddleY = mPoint[1];
    Point2D.Double leftMiddle = new Point2D.Double(leftMiddleX, leftMiddleY);

    double rightMiddleX = this.right;
    double rightMiddleY = mPoint[1];
    Point2D.Double rightMiddle = new Point2D.Double(rightMiddleX, rightMiddleY);

    Point2D.Double middle = new Point2D.Double(mPoint[0], mPoint[1]);

    List<Point2D.Double> rectList = new ArrayList<>();
    rectList.add(this.topLeft);
    rectList.add(middleTop);
    rectList.add(this.topRight);

    rectList.add(leftMiddle);
    rectList.add(middle);
    rectList.add(rightMiddle);

    rectList.add(this.bottomLeft);
    rectList.add(middleBottom);
    rectList.add(this.bottomRight);
    return rectList;
  }
}


/**
 * Store grid data
 */
class GridData {
  public static final int STATUS_CONTAIN = 0;  // contains sub nodes in the polygon.
  public static final int STATUS_ALL = 1;  // all children are in the polygon.
  public static final int STATUS_DISJOIN = 2;  // contains sub nodes in the polygon.

  public long startRow;  // Number of lines started
  public long endRow;  // Number of lines ended
  public long startColumn; // Number of columns started
  public long endColumn;   // Number of columns ended
  private long startHash; // Start hash
  private long endHash;   // End hash
  private int maxDepth;
  private int status = STATUS_DISJOIN;  // Expressing separation

  /**
   * Construct grid area and data
   * @param rs startRow
   * @param re endRow
   * @param cs startColumn
   * @param ce endColumn
   * @param maxDepth Maximum recursion depth
   */
  public GridData(long rs, long re, long cs, long ce, int maxDepth) {
    this.startRow = rs;
    this.endRow = re;
    this.startColumn = cs;
    this.endColumn = ce;
    this.maxDepth = maxDepth;
    computeHashidRange();
  }

  public void setGridData(GridData grid) {
    this.startRow = grid.startRow;
    this.endRow = grid.endRow;
    this.startColumn = grid.startColumn;
    this.endColumn = grid.endColumn;
    this.maxDepth = grid.maxDepth;
    this.startHash = grid.startHash;
    this.endHash = grid.endHash;
    this.status = grid.status;
  }

  /**
   * Construct the range of hashid, construct a start and end range
   */
  private void computeHashidRange() {
    startHash = createHashID(startRow, startColumn);
    endHash = createHashID(endRow - 1, endColumn - 1);
  }

  /**
   * Calculate the corresponding hashid value from the grid coordinates
   * @param row Gridded row index
   * @param column Gridded column index
   * @return In practice, I and j are related to longitude and latitude,
   * and finally the hash value of longitude and latitude data should be brought in
   */
  public long createHashID(long row, long column) {
    long geoID = 0L;
    int bit = 0;
    long sourceRow = row;
    long sourceColumn = column;
    while (sourceRow > 0 || sourceColumn > 0) {
      geoID = geoID | ((sourceRow & 1) << (2 * bit + 1)) | ((sourceColumn & 1) << 2 * bit);
      sourceRow >>= 1;
      sourceColumn >>= 1;
      bit++;
    }
    return geoID;
  }

  /**
   * Set the state of the grid
   * @param status  The allowed input range is STATUS_CONTAIN STATUS_ALL
   */
  public void setStatus(int status) {
    this.status = status;
  }

  /**
   * Get grid status
   * @return grid state
   */
  public int getStatus() {
    return this.status;
  }

  /**
   * Get ID range of grid
   * @return Start ID, end ID
   */
  public Long[] getHashIDRange() {
    return new Long[] {startHash, endHash};
  }
}


/**
 * Nodes of quad tree
 */
class QuadNode {
  private static final Logger LOGGER =
      LogServiceFactory.getLogService(QuadNode.class.getName());
  // The range Z order of region hashid represented by quadtree is a continuous range
  private QuadRect rect;
  // Grid data, actually representing hashid
  private GridData grid;
  // The depth of the current number defaults to 4 ^ n nodes per layer starting from 1
  private int currentDepth;
  // Maximum depth
  private int maxDepth;
  // Scale of point at last division
  private int scale;
  // Hashid range, 0 min, 1 Max
  private QuadNode northWest = null;
  private QuadNode northEast = null;
  private QuadNode southWest = null;
  private QuadNode southEast = null;

  /* Quadrant division of a rectangular region：:

     TL(1)   |    TR(0)
   ----------|-----------
     BL(2)   |    BR(3)
  */

  public enum ChildEnum {
    TOPLEFT, TOPRIGHT, BOTTOMLEFT, BOTTOMRIGHT
  }

  /**
   * build func
   *
   * @param rect         region
   * @param grid         raster data
   * @param currentDepth Current depth
   * @param maxDepth     Maximum depth
   * @param scale        Scale of point at last division
   */
  public QuadNode(QuadRect rect, GridData grid, int currentDepth, int maxDepth, int scale) {
    this.rect = rect;
    this.grid = grid;
    this.currentDepth = currentDepth;
    this.maxDepth = maxDepth;
    this.scale = scale;
  }

  /**
   * Insert a given polygon into a quadtree
   *
   * @param queryPolygon Given polygon area
   */
  public boolean insert(List<double[]> queryPolygon) {
    Polygon polygonGeo = GeometryOperation.getPolygonByDoubleList(queryPolygon);
    if (polygonGeo != null) {
      // Insert polygon into area
      // Before inserting, use the external rectangle to judge whether they are separated.
      // If they are, they are separated from the rectangle. Otherwise, judge the rectangle
      // The functions that enter the insert are inseparable. Judge first in the outer layer
      Geometry rect = polygonGeo.getEnvelope();
      List<Point2D.Double> polygon = this.rect.getPolygonPointList();
      if (GeometryOperation.disjoint(rect, polygon)) {
        LOGGER.info("quad tree disJoint with query polygon envelope return");
        return false;
      } else {
        if (!GeometryOperation.disjoint(polygonGeo, polygon)) {
          LOGGER.info("start to insert query polygon to tree");
          insert(polygonGeo);
          LOGGER.info("end to insert query polygon to tree");
          return true;
        } else {
          LOGGER.info("polygon disJoint with query polygon return");
          return false;
        }
      }
    } else {
      LOGGER.info("query polygon is null return");
      return false;
    }
  }

  /**
   * Insert a region into the point of a quadtree. All the regions that can
   * be inserted are not disjoin regions. They must not be separated
   *
   * @param queryPolygon Area to be inserted
   */
  public void insert(Polygon queryPolygon) {
    List<Point2D.Double> polygon = this.rect.getPolygonPointList();
    Geometry queryRect = queryPolygon.getEnvelope();
    if (isMaxDepth()) {
      // If it is the final grid division, the center point of the grid area will be
      // calculated to determine whether the center point is in the polygon
      // If they are inseparable and not included, they must be intersected and in state.
      // Intersecting indicates partial selection, or they may not be selected when they are
      // the last node
      Point2D.Double middlePoint = this.rect.getMiddlePoint();
      if (!GeometryOperation.disjoint(queryPolygon, middlePoint, true, this.scale)) {
        // Select this area and fill in the data range
        this.grid.setStatus(GridData.STATUS_ALL);
      } else {
        // If not, do nothing
        this.grid.setStatus(GridData.STATUS_DISJOIN);
      }
    } else {
      if (GeometryOperation.contains(queryPolygon, polygon)) {
        // If the point area is included in the area to be queried, the area is selected as a
        // whole. After the area is selected as a whole, the data range needs to be filled in
        this.grid.setStatus(GridData.STATUS_ALL);
      } else {
        // Set state to partially contain
        this.grid.setStatus(GridData.STATUS_CONTAIN);
        // If it is less than the maximum depth, it will cut down and find its corresponding
        // four child nodes directly
        List<Point2D.Double> rectList = this.rect.getSplitRect();
        // Judge the area and create children only when there is intersection. Otherwise, skip.
        // Judge four quadrants respectively
        List<Point2D.Double> topLeft = Arrays.asList(rectList.get(0), rectList.get(1),
            rectList.get(4), rectList.get(3));
        List<Point2D.Double> topRight = Arrays.asList(rectList.get(1), rectList.get(2),
            rectList.get(5), rectList.get(4));
        List<Point2D.Double> bottomLeft = Arrays.asList(rectList.get(3), rectList.get(4),
            rectList.get(7), rectList.get(6));
        List<Point2D.Double> bottomRight = Arrays.asList(rectList.get(4), rectList.get(5),
            rectList.get(8), rectList.get(7));
        long gridRowMiddle = this.grid.startRow + (this.grid.endRow - this.grid.startRow) / 2;
        long gridColumnMiddle = this.grid.startColumn + (this.grid.endColumn -
                                                            this.grid.startColumn) / 2;
        if (!GeometryOperation.disjoint(queryRect, topLeft) && !GeometryOperation
                                               .disjoint(queryPolygon, topLeft)) {
          // If they are not separated, select the upper left half of the mesh
          GridData grid = new GridData(gridRowMiddle, this.grid.endRow, this.grid.startColumn,
              gridColumnMiddle, this.maxDepth);
          insertIntoChildren(ChildEnum.TOPLEFT, grid, topLeft, queryPolygon);
        }
        if (!GeometryOperation.disjoint(queryRect, topRight) && !GeometryOperation
                                               .disjoint(queryPolygon, topRight)) {
          // If not separated, select the upper right half of the mesh
          GridData grid = new GridData(gridRowMiddle, this.grid.endRow, gridColumnMiddle,
              this.grid.endColumn, this.maxDepth);
          insertIntoChildren(ChildEnum.TOPRIGHT, grid, topRight, queryPolygon);
        }
        if (!GeometryOperation.disjoint(queryRect, bottomLeft) && !GeometryOperation
                                                 .disjoint(queryPolygon, bottomLeft)) {
          // If they are not separated, select the lower left half of the mesh
          GridData grid = new GridData(this.grid.startRow, gridRowMiddle, this.grid.startColumn,
              gridColumnMiddle, this.maxDepth);
          insertIntoChildren(ChildEnum.BOTTOMLEFT, grid, bottomLeft, queryPolygon);
        }
        if (!GeometryOperation.disjoint(queryRect, bottomRight) && !GeometryOperation
                                                  .disjoint(queryPolygon, bottomRight)) {
          // If not, select the lower right half of the mesh
          GridData grid = new GridData(this.grid.startRow, gridRowMiddle, gridColumnMiddle,
              this.grid.endColumn, this.maxDepth);
          insertIntoChildren(ChildEnum.BOTTOMRIGHT, grid, bottomRight, queryPolygon);
        }
        // When processing four children, it is necessary to judge whether all four children
        // are selected. If selected, they will be merged
        combineChild();
        // When there is a non empty node and the node status is disjoin, all nodes except this
        // node are null,need to refresh the node state to disjoin and set all its children to
        // be empty
        checkAndSetDisJoin();
      }
    }
  }

  /**
   * Insert grid into tree child node
   *
   * @param childType Child node
   * @param rectangle The area represented by the child's s node
   */
  private void insertIntoChildren(ChildEnum childType, GridData grid,
                                  List<Point2D.Double> rectangle, Polygon queryPolygon) {
    QuadRect rect = new QuadRect(rectangle.get(0), rectangle.get(2));
    switch (childType) {
      case TOPLEFT:
        this.northWest = new QuadNode(rect, grid, currentDepth + 1, maxDepth, scale);
        this.northWest.insert(queryPolygon);
        break;
      case TOPRIGHT:
        this.northEast = new QuadNode(rect, grid, currentDepth + 1, maxDepth, scale);
        this.northEast.insert(queryPolygon);
        break;
      case BOTTOMLEFT:
        this.southWest = new QuadNode(rect, grid, currentDepth + 1, maxDepth, scale);
        this.southWest.insert(queryPolygon);
        break;
      case BOTTOMRIGHT:
        this.southEast = new QuadNode(rect, grid, currentDepth + 1, maxDepth, scale);
        this.southEast.insert(queryPolygon);
        break;
      default:
        LOGGER.warn("child type not match");
    }
  }

  /**
   * Merge child nodes
   */
  private void combineChild() {
    if (checkChildCanCombine(this.northWest) && checkChildCanCombine(this.northEast) &&
            checkChildCanCombine(this.southWest) && checkChildCanCombine(this.southEast)) {
      // Can merge
      this.getGrid().setStatus(GridData.STATUS_ALL);
      this.northWest.clean();
      this.northWest = null;
      this.northEast.clean();
      this.northEast = null;
      this.southWest.clean();
      this.southWest = null;
      this.southEast.clean();
      this.southEast = null;
    }
  }

  /**
   * Determine whether the child node can be merged.
   * When the child node is not empty and the grid data state of the
   * child node is all inclusive, it can be merged
   *
   * @param child Child nodes in the tree
   * @return true Can merge,false Can not merge
   */
  private boolean checkChildCanCombine(QuadNode child) {
    return child != null && child.getGrid().getStatus() == GridData.STATUS_ALL;
  }

  /**
   * Determine whether the region can be set to disjoin state
   */
  private void checkAndSetDisJoin() {
    // Flag allowed to be modified. It is not allowed to be modified by default
    boolean canChange = false;
    // If the status of a child node is status "disjoin, the child node can be left blank
    if (this.northEast != null && this.northEast.getGrid().getStatus() == GridData.STATUS_DISJOIN) {
      this.northEast.clean();
      this.northEast = null;
      canChange = true;
    }
    if (this.northWest != null && this.northWest.getGrid().getStatus() == GridData.STATUS_DISJOIN) {
      this.northWest.clean();
      this.northWest = null;
      canChange = true;
    }
    if (this.southWest != null && this.southWest.getGrid().getStatus() == GridData.STATUS_DISJOIN) {
      this.southWest.clean();
      this.southWest = null;
      canChange = true;
    }
    if (this.southEast != null && this.southEast.getGrid().getStatus() == GridData.STATUS_DISJOIN) {
      this.southEast.clean();
      this.southEast = null;
      canChange = true;
    }
    if (canChange) {
      if (childrenIsNull()) {
        this.getGrid().setStatus(GridData.STATUS_DISJOIN);
      }
    }
  }

  /**
   * Get the area of the node
   *
   * @return rect
   */
  public QuadRect getRect() {
    return rect;
  }

  public GridData getGrid() {
    return grid;
  }

  /**
   * Determine whether the leaf node has been reached
   *
   * @return true Already achieved,false not achieved
   */
  protected boolean isMaxDepth() {
    return currentDepth > maxDepth;
  }

  /**
   * Judge whether the child node is empty
   *
   * @return true Child node is empty, false Child node is not empty
   */
  public boolean childrenIsNull() {
    return this.northWest == null && this.northEast == null && this.southWest == null
               && this.southEast == null;
  }

  /**
   * Clean up the nodes of the tree
   */
  public void clean() {
    rect = null;
    grid = null;
    if (northWest != null) northWest.clean();
    if (northEast != null) northEast.clean();
    if (southWest != null) southWest.clean();
    if (southEast != null) southEast.clean();
  }

  /**
   * Get the child node of a tree node
   *
   * @param childType Enumeration value of child node
   * @return Tree object
   */
  public QuadNode getChildren(ChildEnum childType) {
    switch (childType) {
      case TOPLEFT:
        return this.northWest;
      case TOPRIGHT:
        return this.northEast;
      case BOTTOMRIGHT:
        return this.southEast;
      case BOTTOMLEFT:
        return this.southWest;
      default:
        return null;
    }
  }

  /**
   * Get the status of the current node
   *
   * @return STATUS_CONTAIN 0 or STATUS_ALL 1
   */
  public int getNodeStatus() {
    return grid.getStatus();
  }
}


/**
 * Quad tree object
 */
public class QuadTreeCls {
  private static final Logger LOGGER =
      LogServiceFactory.getLogService(QuadTreeCls.class.getName());
  private QuadNode root;

  /**
   * Create the root node, which contains the entire grid area
   * @param depth Depth of tree
   * @param left Lower left point of coordinate
   * @param down Lower left point of coordinate
   * @param width Width of area
   * @param height Height of area
   * @param scale Scale of point at last division
   */
  public QuadTreeCls(double left, double down, double width, double height, int depth, int scale) {
    QuadRect rect = new QuadRect(left, down,  width,  height);
    // Maximum column length based on depth
    int maxColumn = (int) Math.pow(2, depth);
    // write row,column data to grid use it to build hash id
    GridData grid = new GridData(0, maxColumn, 0, maxColumn, depth);
    root = new QuadNode(rect, grid, 1, depth, scale);
    LOGGER.info("build quad tree successfully, the max column is " + maxColumn);
  }

  /**
   * Given the cutting depth, the region range of quadtree, and the query region, build a quadtree
   * @param vertexes List of polygons given at query time
   */
  public boolean insert(List<double[]> vertexes) {
    if (4 > vertexes.size()) {
      throw new RuntimeException("polygon at least need 4 points, first and last is same.");
    }
    if (!isPointsSame(vertexes.get(0), vertexes.get(vertexes.size() - 1))) {
      throw new RuntimeException("please make first point the same as last point");
    } else {
      // If the initial area is larger than the root node, exit directly
      QuadRect outerRectangle = getOuterRectangle(vertexes.subList(0, vertexes.size() - 1));
      if (this.root.getRect().outsideBox(outerRectangle)) {
        LOGGER.warn("the query outer rect is bigger than the root node return");
        return false;
      }
      return root.insert(vertexes);
    }
  }

  /**
   * Obtain the range of all nodes of tree species
   * @return Scope List
   */
  public List<Long[]> getNodesData() {
    List<Long[]> result = new ArrayList<>();
    getNodeGridRange(root, result);
    sortRange(result);
    combineRange(result);
    LOGGER.info("after query the range size is " + result.size());
    return result;
  }

  /**
   * Get the data range of a node
   * @param node node
   * @param result Return value
   */
  public void getNodeGridRange(QuadNode node, List<Long[]> result) {
    if (node.getNodeStatus() == GridData.STATUS_ALL) {
      Long[] range = node.getGrid().getHashIDRange();
      result.add(range);
    } else {
      // The order of addition is, bottom left, top left, top right, bottom right
      getSubChildGridRange(node, QuadNode.ChildEnum.BOTTOMLEFT, result);
      getSubChildGridRange(node, QuadNode.ChildEnum.TOPLEFT, result);
      getSubChildGridRange(node, QuadNode.ChildEnum.TOPRIGHT, result);
      getSubChildGridRange(node, QuadNode.ChildEnum.BOTTOMRIGHT, result);
    }
  }

  /**
   * Get the range of a node's child nodes
   * @param node tree node
   * @param childType Child node type
   * @param result  return value
   */
  public void getSubChildGridRange(QuadNode node, QuadNode.ChildEnum childType,
                                   List<Long[]> result) {
    QuadNode child = node.getChildren(childType);
    if (child != null) {
      getNodeGridRange(child, result);
    }
  }

  /**
   * Sorting and merging area results
   * @param rangeList Region list
   */
  public void sortRange(List<Long[]> rangeList) {
    // When sorting, you only need to sort the first node of long [],
    // because you can ensure that the interval is not repeated
    rangeList.sort(new Comparator<Long[]>() {
      @Override
      public int compare(Long[] x, Long[] y) {
        return Long.compare(x[0], y[0]);
      }
    });
  }

  /**
   * If the ordered data is merged, the number of data segments after merging may be reduced.
   * @param rangeList Area list, sorted
   */
  public void combineRange(List<Long[]> rangeList) {
    if (rangeList.size() > 1) {
      for (int i = 0, j = i + 1; i < rangeList.size() - 1; i++, j++) {
        long previousEnd = rangeList.get(i)[1];
        long nextStart = rangeList.get(j)[0];
        if (previousEnd + 1 == nextStart) {
          rangeList.get(j)[0] = rangeList.get(i)[0];
          rangeList.get(i)[0] = null;
          rangeList.get(i)[1] = null;
        }
      }
      rangeList.removeIf(item -> item[0] == null && item[1] == null);
    }
  }

  /**
   * Get the circumscribed rectangle of a polygon
   * @param polygon polygon
   * @return Rectangular area
   */
  private QuadRect getOuterRectangle(List<double[]> polygon) {
    double left = Double.MAX_VALUE;
    double top = Double.MIN_VALUE;
    double right = Double.MIN_VALUE;
    double bottom = Double.MAX_VALUE;
    for (double[] point : polygon) {
      if (point[0] < left) {
        left = point[0];
      }
      if (point[0] > right) {
        right = point[0];
      }
      if (point[1] < bottom) {
        bottom = point[1];
      }
      if (point[1] > top) {
        top = point[1];
      }
    }
    Point2D.Double topLeft = new Point2D.Double(left, top);
    Point2D.Double bottomRight = new Point2D.Double(right, bottom);
    return new QuadRect(topLeft, bottomRight);
  }

  public QuadNode getRoot() {
    return root;
  }

  public void clean() {
    if (root != null) {
      root.clean();
      root = null;
    }
  }

  /**
   * check the 2 point is same.
   * because the queryList has been checked so no need to check x and y
   * @param x a double[] has 2 data
   * @param y a double[] has 2 data
   * @return true 2 point is same, false 2 point is not same
   */
  private boolean isPointsSame(double[] x, double[] y) {
    return Double.toString(x[0]).equals(Double.toString(y[0]))
               && Double.toString(x[1]).equals(Double.toString(y[1]));
  }
}







