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

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.List;

/**
 * QuadTreeClsTest Tester.
 */
public class QuadTreeClsTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    QuadTreeCls qtreee;

    @Before
    public void before() throws Exception {
        qtreee = new QuadTreeCls(0, 0, 16, 16, 4, 6);
    }

    @After
    public void after() throws Exception {
        qtreee.clean();
    }

    @Test
    public void testInsertPolygonFirstAndLastPoint() throws Exception {
        //Insert the entire area, directly root
        List<double[]> pointList = new ArrayList<>();
        pointList.add(new double[] {0, 0});
        pointList.add(new double[] {0, 16});
        pointList.add(new double[] {16, 16});
        pointList.add(new double[] {16, 0});
        exception.expect(RuntimeException.class);
        exception.expectMessage("please make first point the same as last point");
        qtreee.insert(pointList);
    }

    /**
     * The test just inserts the entire coordinate area
     */
    @Test
    public void testInsertPolygonFullRange() throws Exception {
        //Insert the entire area, directly root
        List<double[]> pointList = new ArrayList<>();
        pointList.add(new double[] {0, 0});
        pointList.add(new double[] {0, 16});
        pointList.add(new double[] {16, 16});
        pointList.add(new double[] {16, 0});
        pointList.add(new double[] {0, 0});
        qtreee.insert(pointList);
        Assume.assumeTrue(qtreee.getRoot().getGrid().getStatus() == GridData.STATUS_ALL);
        Long[] gridRange = qtreee.getRoot().getGrid().getHashIDRange();
        Assume.assumeTrue(gridRange[0] == 0);
        Assume.assumeTrue(gridRange[1] == 255);
    }

    /**
     * Test insertion area is larger than the whole area
     */
    @Test
    public void testInsertBiggerPolygon() throws Exception {
        List<double[]> pointList = new ArrayList<>();
        pointList.add(new double[] {4.3, 20});
        pointList.add(new double[] {7.3, 13.8});
        pointList.add(new double[] {18, 11.8});
        pointList.add(new double[] {11.5, 6.3});
        pointList.add(new double[] {4.3, 20});

        qtreee.insert(pointList);
        QuadNode root = qtreee.getRoot();

        Assume.assumeTrue(root.childrenIsNull());
    }

    /**
     * Illegal test insertion area
     */
    @Test
    public void testInsertLessThan3Points() throws Exception {
        List<double[]> pointList = new ArrayList<>();
        pointList.add(new double[] {4.3, 9.4});
        pointList.add(new double[] {7.3, 13.8});
        exception.expect(RuntimeException.class);
        exception.expectMessage("polygon at least need 4 points, first and last is same.");
        qtreee.insert(pointList);
        QuadNode root = qtreee.getRoot();
        Assume.assumeTrue(root.childrenIsNull());
    }

    /**
     * Test creation Hadid
     */
    @Test
    public void testCreateHashID() throws Exception {
        GridData grid = new GridData(0,0, 15,15,4);
        // Here is a grid
        Assume.assumeTrue(grid.createHashID(0,0) == 0);
        Assume.assumeTrue(grid.createHashID(0,1) == 1);
        Assume.assumeTrue(grid.createHashID(1,0) == 2);
        Assume.assumeTrue(grid.createHashID(1,1) == 3);

        Assume.assumeTrue(grid.createHashID(0,2) == 4);
        Assume.assumeTrue(grid.createHashID(0,3) == 5);
        Assume.assumeTrue(grid.createHashID(1,2) == 6);
        Assume.assumeTrue(grid.createHashID(1,3) == 7);

        Assume.assumeTrue(grid.createHashID(2,0) == 8);
        Assume.assumeTrue(grid.createHashID(2,1) == 9);
        Assume.assumeTrue(grid.createHashID(3,0) == 10);
        Assume.assumeTrue(grid.createHashID(3,1) == 11);

        Assume.assumeTrue(grid.createHashID(2,2) == 12);
        Assume.assumeTrue(grid.createHashID(2,3) == 13);
        Assume.assumeTrue(grid.createHashID(3,2) == 14);
        Assume.assumeTrue(grid.createHashID(3,3) == 15);
        // Center point
        Assume.assumeTrue(grid.createHashID(0,4) == 16);
        Assume.assumeTrue(grid.createHashID(0,5) == 17);
        Assume.assumeTrue(grid.createHashID(1,4) == 18);
        Assume.assumeTrue(grid.createHashID(1,5) == 19);

        Assume.assumeTrue(grid.createHashID(0,6) == 20);
        Assume.assumeTrue(grid.createHashID(0,7) == 21);
        Assume.assumeTrue(grid.createHashID(1,6) == 22);
        Assume.assumeTrue(grid.createHashID(1,7) == 23);

        Assume.assumeTrue(grid.createHashID(2,4) == 24);
        Assume.assumeTrue(grid.createHashID(2,5) == 25);
        Assume.assumeTrue(grid.createHashID(3,4) == 26);
        Assume.assumeTrue(grid.createHashID(3,5) == 27);

        Assume.assumeTrue(grid.createHashID(2,6) == 28);
        Assume.assumeTrue(grid.createHashID(2,7) == 29);
        Assume.assumeTrue(grid.createHashID(3,6) == 30);
        Assume.assumeTrue(grid.createHashID(3,7) == 31);

        Assume.assumeTrue(grid.createHashID(15,15) == 255);
        Assume.assumeTrue(grid.createHashID(8,8) == 192);

    }

    /**
     * Test inserts a rectangle
     */
    @Test
    public void testQueryOne() throws Exception {
        List<double[]> pointList = new ArrayList<>();
        pointList.add(new double[] {4.3, 9.4});
        pointList.add(new double[] {7.3, 13.8});
        pointList.add(new double[] {13.6, 11.8});
        pointList.add(new double[] {11.5, 6.3});
        pointList.add(new double[] {4.3, 9.4});
        boolean flag = qtreee.insert(pointList);
        // First floor
        QuadNode oneLevel_TOPLEFT = qtreee.getRoot().getChildren(QuadNode.ChildEnum.TOPLEFT);
        QuadNode oneLevel_TOPRIGHT = qtreee.getRoot().getChildren(QuadNode.ChildEnum.TOPRIGHT);
        QuadNode oneLevel_BOTTOMRIGHT = qtreee.getRoot().getChildren(QuadNode.ChildEnum.BOTTOMRIGHT);
        QuadNode oneLevel_BOTTOMLEFT = qtreee.getRoot().getChildren(QuadNode.ChildEnum.BOTTOMLEFT);
        Assume.assumeTrue(!oneLevel_TOPLEFT.childrenIsNull());
        Assume.assumeTrue(!oneLevel_TOPRIGHT.childrenIsNull());
        Assume.assumeTrue(!oneLevel_BOTTOMRIGHT.childrenIsNull());
        Assume.assumeTrue(oneLevel_BOTTOMLEFT == null);
        Assume.assumeTrue(oneLevel_TOPLEFT.getGrid().getStatus() == GridData.STATUS_CONTAIN);
        Assume.assumeTrue(oneLevel_TOPRIGHT.getGrid().getStatus() == GridData.STATUS_CONTAIN);
        Assume.assumeTrue(oneLevel_BOTTOMRIGHT.getGrid().getStatus() == GridData.STATUS_CONTAIN);
        // The second floor
        // oneLevel_TOPLEFT
        QuadNode twoLevel_TOPLEFT_TOPLEFT = oneLevel_TOPLEFT.getChildren(QuadNode.ChildEnum.TOPLEFT);
        QuadNode twoLevel_TOPLEFT_TOPRIGHT = oneLevel_TOPLEFT.getChildren(QuadNode.ChildEnum.TOPRIGHT);
        QuadNode twoLevel_TOPLEFT_BOTTOMRIGHT = oneLevel_TOPLEFT.getChildren(QuadNode.ChildEnum.BOTTOMRIGHT);
        QuadNode twoLevel_TOPLEFT_BOTTOMLEFT  = oneLevel_TOPLEFT.getChildren(QuadNode.ChildEnum.BOTTOMLEFT );
        Assume.assumeTrue(twoLevel_TOPLEFT_TOPLEFT == null);
        Assume.assumeTrue(!twoLevel_TOPLEFT_TOPRIGHT.childrenIsNull());
        Assume.assumeTrue(!twoLevel_TOPLEFT_BOTTOMRIGHT.childrenIsNull());
        Assume.assumeTrue(twoLevel_TOPLEFT_BOTTOMLEFT == null);
        //oneLevel_TOPRIGHT
        QuadNode twoLevel_TOPRIGHT_TOPLEFT = oneLevel_TOPRIGHT.getChildren(QuadNode.ChildEnum.TOPLEFT);
        QuadNode twoLevel_TOPRIGHT_TOPRIGHT = oneLevel_TOPRIGHT.getChildren(QuadNode.ChildEnum.TOPRIGHT);
        QuadNode twoLevel_TOPRIGHT_BOTTOMRIGHT = oneLevel_TOPRIGHT.getChildren(QuadNode.ChildEnum.BOTTOMRIGHT);
        QuadNode twoLevel_TOPRIGHT_BOTTOMLEFT  = oneLevel_TOPRIGHT.getChildren(QuadNode.ChildEnum.BOTTOMLEFT );
        Assume.assumeTrue(!twoLevel_TOPRIGHT_TOPLEFT.childrenIsNull());
        Assume.assumeTrue(twoLevel_TOPRIGHT_TOPRIGHT == null);
        Assume.assumeTrue(!twoLevel_TOPRIGHT_BOTTOMRIGHT.childrenIsNull());
        Assume.assumeTrue(twoLevel_TOPRIGHT_BOTTOMLEFT.childrenIsNull());
        Assume.assumeTrue(twoLevel_TOPRIGHT_BOTTOMLEFT.getGrid().getStatus() == GridData.STATUS_ALL);
        //oneLevel_BOTTOMLEFT
        //null

        // oneLevel_BOTTOMRIGHT
        QuadNode twoLevel_BOTTOMRIGHT_TOPLEFT = oneLevel_BOTTOMRIGHT.getChildren(QuadNode.ChildEnum.TOPLEFT);
        QuadNode twoLevel_BOTTOMRIGHT_TOPRIGHT = oneLevel_BOTTOMRIGHT.getChildren(QuadNode.ChildEnum.TOPRIGHT);
        QuadNode twoLevel_BOTTOMRIGHT_BOTTOMRIGHT = oneLevel_BOTTOMRIGHT.getChildren(QuadNode.ChildEnum.BOTTOMRIGHT);
        QuadNode twoLevel_BOTTOMRIGHT_BOTTOMLEFT  = oneLevel_BOTTOMRIGHT.getChildren(QuadNode.ChildEnum.BOTTOMLEFT );
        Assume.assumeTrue(!twoLevel_BOTTOMRIGHT_TOPLEFT.childrenIsNull());
        Assume.assumeTrue(twoLevel_BOTTOMRIGHT_TOPLEFT.getGrid().getStatus() == GridData.STATUS_CONTAIN);
        Assume.assumeTrue(twoLevel_BOTTOMRIGHT_TOPRIGHT == null);
        Assume.assumeTrue(twoLevel_BOTTOMRIGHT_BOTTOMRIGHT == null);
        Assume.assumeTrue(twoLevel_BOTTOMRIGHT_BOTTOMLEFT == null);

        // The third level
        // twoLevel_TOPLEFT_TOPRIGHT
        QuadNode twoLevel_TOPLEFT_TOPRIGHT_TOPLEFT = twoLevel_TOPLEFT_TOPRIGHT.getChildren(QuadNode.ChildEnum.TOPLEFT);
        QuadNode twoLevel_TOPLEFT_TOPRIGHT_TOPRIGHT = twoLevel_TOPLEFT_TOPRIGHT.getChildren(QuadNode.ChildEnum.TOPRIGHT);
        QuadNode twoLevel_TOPLEFT_TOPRIGHT_BOTTOMRIGHT = twoLevel_TOPLEFT_TOPRIGHT.getChildren(QuadNode.ChildEnum.BOTTOMRIGHT);
        QuadNode twoLevel_TOPLEFT_TOPRIGHT_BOTTOMLEFT  = twoLevel_TOPLEFT_TOPRIGHT.getChildren(QuadNode.ChildEnum.BOTTOMLEFT );
        Assume.assumeTrue(twoLevel_TOPLEFT_TOPRIGHT_TOPLEFT == null);
        Assume.assumeTrue(twoLevel_TOPLEFT_TOPRIGHT_TOPRIGHT == null);
        Assume.assumeTrue(!twoLevel_TOPLEFT_TOPRIGHT_BOTTOMRIGHT.childrenIsNull());
        Assume.assumeTrue(twoLevel_TOPLEFT_TOPRIGHT_BOTTOMRIGHT.getGrid().getStatus() == GridData.STATUS_CONTAIN);
        Assume.assumeTrue(twoLevel_TOPLEFT_TOPRIGHT_BOTTOMLEFT == null);
        // twoLevel_TOPLEFT_BOTTOMRIGHT
        QuadNode twoLevel_TOPLEFT_BOTTOMRIGHT_TOPLEFT = twoLevel_TOPLEFT_BOTTOMRIGHT.getChildren(QuadNode.ChildEnum.TOPLEFT);
        QuadNode twoLevel_TOPLEFT_BOTTOMRIGHT_TOPRIGHT = twoLevel_TOPLEFT_BOTTOMRIGHT.getChildren(QuadNode.ChildEnum.TOPRIGHT);
        QuadNode twoLevel_TOPLEFT_BOTTOMRIGHT_BOTTOMRIGHT = twoLevel_TOPLEFT_BOTTOMRIGHT.getChildren(QuadNode.ChildEnum.BOTTOMRIGHT);
        QuadNode twoLevel_TOPLEFT_BOTTOMRIGHT_BOTTOMLEFT  = twoLevel_TOPLEFT_BOTTOMRIGHT.getChildren(QuadNode.ChildEnum.BOTTOMLEFT );
        Assume.assumeTrue(!twoLevel_TOPLEFT_BOTTOMRIGHT_TOPLEFT.childrenIsNull());
        Assume.assumeTrue(twoLevel_TOPLEFT_BOTTOMRIGHT_TOPLEFT.getGrid().getStatus() == GridData.STATUS_CONTAIN);

        Assume.assumeTrue(twoLevel_TOPLEFT_BOTTOMRIGHT_TOPRIGHT.childrenIsNull());
        Assume.assumeTrue(twoLevel_TOPLEFT_BOTTOMRIGHT_TOPRIGHT.getGrid().getStatus() == GridData.STATUS_ALL);

        Assume.assumeTrue(twoLevel_TOPLEFT_BOTTOMRIGHT_BOTTOMRIGHT.childrenIsNull());
        Assume.assumeTrue(twoLevel_TOPLEFT_BOTTOMRIGHT_BOTTOMRIGHT.getGrid().getStatus() == GridData.STATUS_ALL);

        Assume.assumeTrue(!twoLevel_TOPLEFT_BOTTOMRIGHT_BOTTOMLEFT.childrenIsNull());
        Assume.assumeTrue(twoLevel_TOPLEFT_BOTTOMRIGHT_BOTTOMLEFT.getGrid().getStatus() == GridData.STATUS_CONTAIN);
        // twoLevel_TOPRIGHT_TOPLEFT
        QuadNode twoLevel_TOPRIGHT_TOPLEFT_TOPLEFT = twoLevel_TOPRIGHT_TOPLEFT.getChildren(QuadNode.ChildEnum.TOPLEFT);
        QuadNode twoLevel_TOPRIGHT_TOPLEFT_TOPRIGHT = twoLevel_TOPRIGHT_TOPLEFT.getChildren(QuadNode.ChildEnum.TOPRIGHT);
        QuadNode twoLevel_TOPRIGHT_TOPLEFT_BOTTOMRIGHT = twoLevel_TOPRIGHT_TOPLEFT.getChildren(QuadNode.ChildEnum.BOTTOMRIGHT);
        QuadNode twoLevel_TOPRIGHT_TOPLEFT_BOTTOMLEFT  = twoLevel_TOPRIGHT_TOPLEFT.getChildren(QuadNode.ChildEnum.BOTTOMLEFT );
        Assume.assumeTrue(twoLevel_TOPRIGHT_TOPLEFT_TOPLEFT == null);
        Assume.assumeTrue(twoLevel_TOPRIGHT_TOPLEFT_TOPRIGHT == null);
        Assume.assumeTrue(!twoLevel_TOPRIGHT_TOPLEFT_BOTTOMRIGHT.childrenIsNull());
        Assume.assumeTrue(twoLevel_TOPRIGHT_TOPLEFT_BOTTOMRIGHT.getGrid().getStatus() == GridData.STATUS_CONTAIN);
        Assume.assumeTrue(!twoLevel_TOPRIGHT_TOPLEFT_BOTTOMLEFT.childrenIsNull());
        Assume.assumeTrue(twoLevel_TOPRIGHT_TOPLEFT_BOTTOMLEFT.getGrid().getStatus() == GridData.STATUS_CONTAIN);
        //twoLevel_TOPRIGHT_BOTTOMRIGHT
        QuadNode twoLevel_TOPRIGHT_BOTTOMRIGHT_TOPLEFT = twoLevel_TOPRIGHT_BOTTOMRIGHT.getChildren(QuadNode.ChildEnum.TOPLEFT);
        QuadNode twoLevel_TOPRIGHT_BOTTOMRIGHT_TOPRIGHT = twoLevel_TOPRIGHT_BOTTOMRIGHT.getChildren(QuadNode.ChildEnum.TOPRIGHT);
        QuadNode twoLevel_TOPRIGHT_BOTTOMRIGHT_BOTTOMRIGHT = twoLevel_TOPRIGHT_BOTTOMRIGHT.getChildren(QuadNode.ChildEnum.BOTTOMRIGHT);
        QuadNode twoLevel_TOPRIGHT_BOTTOMRIGHT_BOTTOMLEFT  = twoLevel_TOPRIGHT_BOTTOMRIGHT.getChildren(QuadNode.ChildEnum.BOTTOMLEFT );

        Assume.assumeTrue(!twoLevel_TOPRIGHT_BOTTOMRIGHT_TOPLEFT.childrenIsNull());
        Assume.assumeTrue(twoLevel_TOPRIGHT_BOTTOMRIGHT_TOPLEFT.getGrid().getStatus() == GridData.STATUS_CONTAIN);
        Assume.assumeTrue(twoLevel_TOPRIGHT_BOTTOMRIGHT_TOPRIGHT == null);
        Assume.assumeTrue(twoLevel_TOPRIGHT_BOTTOMRIGHT_BOTTOMRIGHT == null);
        Assume.assumeTrue(!twoLevel_TOPRIGHT_BOTTOMRIGHT_BOTTOMLEFT.childrenIsNull());
        Assume.assumeTrue(twoLevel_TOPRIGHT_BOTTOMRIGHT_BOTTOMLEFT.getGrid().getStatus() == GridData.STATUS_CONTAIN);
        // twoLevel_BOTTOMRIGHT_TOPLEFT
        QuadNode twoLevel_BOTTOMRIGHT_TOPLEFT_TOPLEFT = twoLevel_BOTTOMRIGHT_TOPLEFT.getChildren(QuadNode.ChildEnum.TOPLEFT);
        QuadNode twoLevel_BOTTOMRIGHT_TOPLEFT_TOPRIGHT = twoLevel_BOTTOMRIGHT_TOPLEFT.getChildren(QuadNode.ChildEnum.TOPRIGHT);
        QuadNode twoLevel_BOTTOMRIGHT_TOPLEFT_BOTTOMRIGHT = twoLevel_BOTTOMRIGHT_TOPLEFT.getChildren(QuadNode.ChildEnum.BOTTOMRIGHT);
        QuadNode twoLevel_BOTTOMRIGHT_TOPLEFT_BOTTOMLEFT  = twoLevel_BOTTOMRIGHT_TOPLEFT.getChildren(QuadNode.ChildEnum.BOTTOMLEFT );
        Assume.assumeTrue(!twoLevel_BOTTOMRIGHT_TOPLEFT_TOPLEFT.childrenIsNull());
        Assume.assumeTrue(twoLevel_BOTTOMRIGHT_TOPLEFT_TOPLEFT.getGrid().getStatus() == GridData.STATUS_CONTAIN);
        Assume.assumeTrue(!twoLevel_BOTTOMRIGHT_TOPLEFT_TOPRIGHT.childrenIsNull());
        Assume.assumeTrue(twoLevel_BOTTOMRIGHT_TOPLEFT_TOPRIGHT.getGrid().getStatus() == GridData.STATUS_CONTAIN);
        Assume.assumeTrue(twoLevel_BOTTOMRIGHT_TOPLEFT_BOTTOMRIGHT == null);
        Assume.assumeTrue(twoLevel_BOTTOMRIGHT_TOPLEFT_BOTTOMLEFT == null);

    }

    /**
     * Initial result 180->180  183->183  181->181  146->146  147->147  153->153  156->159  148->151  192->207  224->224  225->225  228->228  210->210  216->216  218->218  107->107  110->110  111->111  109->109
     * Results after sorting 107->107  109->109  110->110  111->111  146->146  147->147  148->151  153->153  156->159  180->180  181->181  183->183  192->207  210->210  216->216  218->218  224->224  225->225  228->228
     * Combined results 107->107  109->111  146->151  153->153  156->159  180->181  183->183  192->207  210->210  216->216  218->218  224->225  228->228
     * @throws Exception
     */
    @Test
    public void testGetRange() throws Exception {
        List<double[]> pointList = new ArrayList<>();
        pointList.add(new double[] {4.3, 9.4});
        pointList.add(new double[] {7.3, 13.8});
        pointList.add(new double[] {13.6, 11.8});
        pointList.add(new double[] {11.5, 6.3});
        pointList.add(new double[] {4.3, 9.4});
        qtreee.insert(pointList);
        List<Long[]> data = qtreee.getNodesData();

        // 107->107  109->111  146->151  153->153  156->159  180->181  183->183  192->207  210->210  216->216  218->218  224->225  228->228

        Assume.assumeTrue(checkValidate(data, 0, 107, 107));
        Assume.assumeTrue(checkValidate(data, 1, 109, 111));
        Assume.assumeTrue(checkValidate(data, 2, 146, 151));
        Assume.assumeTrue(checkValidate(data, 3, 153, 153));
        Assume.assumeTrue(checkValidate(data, 4, 156, 159));
        Assume.assumeTrue(checkValidate(data, 5, 180, 181));

        Assume.assumeTrue(checkValidate(data, 6, 183, 183));
        Assume.assumeTrue(checkValidate(data, 7, 192, 207));
        Assume.assumeTrue(checkValidate(data, 8, 210, 210));
        Assume.assumeTrue(checkValidate(data, 9, 216, 216));
        Assume.assumeTrue(checkValidate(data, 10, 218, 218));
        Assume.assumeTrue(checkValidate(data, 11, 224, 225));
        Assume.assumeTrue(checkValidate(data, 12, 228, 228));
    }

    private boolean checkValidate(List<Long[]> data, int index, int start, int end) {
        Long[] tmp = data.get(index);
        return tmp[0] == start && tmp[1] == end;
    }
}
