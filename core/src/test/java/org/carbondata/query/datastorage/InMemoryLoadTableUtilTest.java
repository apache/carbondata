package org.carbondata.query.datastorage;

import java.io.ByteArrayInputStream;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.*;

import org.carbondata.core.carbon.CarbonDef;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.load.LoadMetadataDetails;
import org.carbondata.core.util.CarbonProperties;
import org.eigenbase.xom.Parser;
import org.eigenbase.xom.XOMException;
import org.eigenbase.xom.XOMUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * The class is to test the common utility method of InMemoryLoadTableUtil
 */
public class InMemoryLoadTableUtilTest {

    @Test public void createLoadNameAndStatusMapping() throws Exception {
        LoadMetadataDetails[] loadMetadataDetails = new LoadMetadataDetails[3];
        loadMetadataDetails[0] = new LoadMetadataDetails();
        loadMetadataDetails[0].setLoadName("0");
        loadMetadataDetails[0].setLoadStatus(CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS);
        loadMetadataDetails[0].setTimestamp("01-04-2016 07:43:00");
        loadMetadataDetails[0].setLoadStartTime("01-04-2016 07:42:58");
        loadMetadataDetails[1] = new LoadMetadataDetails();
        loadMetadataDetails[1].setLoadName("1");
        loadMetadataDetails[1]
                .setLoadStatus(CarbonCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS);
        loadMetadataDetails[1].setTimestamp("01-04-2016 07:53:00");
        loadMetadataDetails[1].setLoadStartTime("01-04-2016 07:54:58");
        loadMetadataDetails[2] = new LoadMetadataDetails();
        loadMetadataDetails[2].setLoadName("2");
        loadMetadataDetails[2].setLoadStatus(CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS);
        loadMetadataDetails[2].setTimestamp("01-04-2016 07:58:00");
        loadMetadataDetails[2].setLoadStartTime("01-04-2016 07:59:58");

        Map<String, String> actualLoadNameAndStatusMapping =
                InMemoryLoadTableUtil.createLoadNameAndStatusMapping(loadMetadataDetails);
        Map<String, String> expectedLoadNameStatusMap =
                new HashMap<String, String>(loadMetadataDetails.length);
        expectedLoadNameStatusMap.put(CarbonCommonConstants.LOAD_FOLDER + '0',
                CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS);
        expectedLoadNameStatusMap.put(CarbonCommonConstants.LOAD_FOLDER + '1',
                CarbonCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS);
        expectedLoadNameStatusMap.put(CarbonCommonConstants.LOAD_FOLDER + '2',
                CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS);
        Set<Map.Entry<String, String>> entries = expectedLoadNameStatusMap.entrySet();
        Iterator<Map.Entry<String, String>> iterator = entries.iterator();
        boolean isEqual = true;
        while (iterator.hasNext()) {
            Map.Entry<String, String> next = iterator.next();
            String key = next.getKey();
            String expectedStatus = next.getValue();
            String actualStatus = actualLoadNameAndStatusMapping.get(key);
            if (!expectedStatus.equalsIgnoreCase(actualStatus)) {
                isEqual = false;
                break;
            }
        }
        Assert.assertTrue("Actual LoadNameAndStatus map does not match with the expected map : ",
                isEqual);
    }

    @Test public void createLoadAndModificationTimeMappping() throws Exception {
        {
            LoadMetadataDetails[] loadMetadataDetails = new LoadMetadataDetails[3];
            loadMetadataDetails[0] = new LoadMetadataDetails();
            loadMetadataDetails[0].setLoadName("0");
            loadMetadataDetails[0].setLoadStatus(CarbonCommonConstants.MARKED_FOR_UPDATE);
            loadMetadataDetails[0].setTimestamp("01-04-2016 07:43:00");
            SimpleDateFormat carbonDateFormat =
                    new SimpleDateFormat(CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT);
            String modificationOrdeletionTimesStamp =
                    carbonDateFormat.format(new Date()).toString();
            loadMetadataDetails[0]
                    .setModificationOrdeletionTimesStamp(modificationOrdeletionTimesStamp);
            loadMetadataDetails[0].setLoadStartTime("01-04-2016 07:42:58");
            loadMetadataDetails[1] = new LoadMetadataDetails();
            loadMetadataDetails[1].setLoadName("1");
            loadMetadataDetails[1]
                    .setLoadStatus(CarbonCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS);
            loadMetadataDetails[1].setTimestamp("01-04-2016 07:53:00");
            loadMetadataDetails[1].setLoadStartTime("01-04-2016 07:54:58");
            loadMetadataDetails[2] = new LoadMetadataDetails();
            loadMetadataDetails[2].setLoadName("2");
            loadMetadataDetails[2].setLoadStatus(CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS);
            loadMetadataDetails[2].setTimestamp("01-04-2016 07:58:00");
            loadMetadataDetails[2].setLoadStartTime("01-04-2016 07:59:58");

            Map<String, Long> actualLoadNameModificationTimeMap = InMemoryLoadTableUtil
                    .createLoadAndModificationTimeMappping(loadMetadataDetails);
            Map<String, Long> expectedLoadNameAndModTimeMap =
                    new HashMap<String, Long>(loadMetadataDetails.length);
            expectedLoadNameAndModTimeMap.put(CarbonCommonConstants.LOAD_FOLDER + '0',
                    carbonDateFormat.parse(modificationOrdeletionTimesStamp).getTime());
            expectedLoadNameAndModTimeMap.put(CarbonCommonConstants.LOAD_FOLDER + '1', 0L);
            expectedLoadNameAndModTimeMap.put(CarbonCommonConstants.LOAD_FOLDER + '2', 0L);
            Set<Map.Entry<String, Long>> entries = expectedLoadNameAndModTimeMap.entrySet();
            Iterator<Map.Entry<String, Long>> iterator = entries.iterator();
            boolean isEqual = true;
            while (iterator.hasNext()) {
                Map.Entry<String, Long> next = iterator.next();
                String key = next.getKey();
                Long expectedTimeStamp = next.getValue();
                Long modificationTimeStamp = actualLoadNameModificationTimeMap.get(key);
                if (!expectedTimeStamp.equals(modificationTimeStamp)) {
                    isEqual = false;
                    break;
                }
            }
            Assert.assertTrue(
                    "Actual LoadNameAndModificationTimeStamp map does not match with the expected "
                            + "map : ", isEqual);
        }

    }

    @Test public void getListOfFoldersToBeLoaded() throws Exception {
        LoadMetadataDetails[] loadMetadataDetails = new LoadMetadataDetails[3];
        loadMetadataDetails[0] = new LoadMetadataDetails();
        loadMetadataDetails[0].setLoadName("0");
        loadMetadataDetails[0].setLoadStatus(CarbonCommonConstants.MARKED_FOR_UPDATE);
        loadMetadataDetails[0].setTimestamp("01-04-2016 07:43:00");
        SimpleDateFormat carbonDateFormat =
                new SimpleDateFormat(CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT);
        String modificationOrdeletionTimesStamp = carbonDateFormat.format(new Date()).toString();
        loadMetadataDetails[0]
                .setModificationOrdeletionTimesStamp(modificationOrdeletionTimesStamp);
        loadMetadataDetails[0].setLoadStartTime("01-04-2016 07:42:58");
        loadMetadataDetails[1] = new LoadMetadataDetails();
        loadMetadataDetails[1].setLoadName("1");
        loadMetadataDetails[1]
                .setLoadStatus(CarbonCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS);
        loadMetadataDetails[1].setTimestamp("01-04-2016 07:53:00");
        loadMetadataDetails[1].setLoadStartTime("01-04-2016 07:54:58");
        loadMetadataDetails[2] = new LoadMetadataDetails();
        loadMetadataDetails[2].setLoadName("2");
        loadMetadataDetails[2].setLoadStatus(CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS);
        loadMetadataDetails[2].setTimestamp("01-04-2016 07:58:00");
        loadMetadataDetails[2].setLoadStartTime("01-04-2016 07:59:58");
        Map<String, String> loadNameAndStatusMapping =
                InMemoryLoadTableUtil.createLoadNameAndStatusMapping(loadMetadataDetails);
        List<String> listLoadFolders = new ArrayList<String>();
        listLoadFolders.add(CarbonCommonConstants.LOAD_FOLDER + "0");
        listLoadFolders.add(CarbonCommonConstants.LOAD_FOLDER + "1");
        listLoadFolders.add(CarbonCommonConstants.LOAD_FOLDER + "2");
        List<RestructureStore> slices = new ArrayList<RestructureStore>();
        String factTableName = "alldatatypescube_1";
        List<String> listOfFoldersToBeLoaded = InMemoryLoadTableUtil
                .getListOfFoldersToBeLoaded(listLoadFolders, loadNameAndStatusMapping, slices,
                        factTableName, loadMetadataDetails);
        int expectedSize = listLoadFolders.size();
        int actualSize = listOfFoldersToBeLoaded.size();
        Assert.assertEquals(expectedSize, actualSize);

        RestructureStore restructureStore = new RestructureStore("RS_", 0);
        CarbonDef.Schema schema = getSchema();
        InMemoryTable inMemoryTable =
                new InMemoryTable(schema, schema.cubes[0], null, factTableName, "",
                        carbonDateFormat.parse(modificationOrdeletionTimesStamp).getTime());
        inMemoryTable.setLoadName(CarbonCommonConstants.LOAD_FOLDER + "0");
        Map<String, List<InMemoryTable>> mapOfSlices = new HashMap<>();
        List<InMemoryTable> lisOfSlices = new ArrayList<>();
        lisOfSlices.add(inMemoryTable);
        mapOfSlices.put(factTableName, lisOfSlices);
        restructureStore.setSlices(mapOfSlices);
        slices.add(restructureStore);
        List<String> listOfFoldersToBeLoaded1 = InMemoryLoadTableUtil
                .getListOfFoldersToBeLoaded(listLoadFolders, loadNameAndStatusMapping, slices,
                        factTableName, loadMetadataDetails);
        Assert.assertEquals(2, listOfFoldersToBeLoaded1.size());
    }

    @Test public void checkAndDeleteStaleSlices() throws Exception {
        LoadMetadataDetails[] loadMetadataDetails = new LoadMetadataDetails[3];
        loadMetadataDetails[0] = new LoadMetadataDetails();
        loadMetadataDetails[0].setLoadName("0");
        loadMetadataDetails[0].setLoadStatus(CarbonCommonConstants.MARKED_FOR_UPDATE);
        loadMetadataDetails[0].setTimestamp("01-04-2016 07:43:00");
        SimpleDateFormat carbonDateFormat =
                new SimpleDateFormat(CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT);
        String modificationOrdeletionTimesStamp = carbonDateFormat.format(new Date()).toString();
        loadMetadataDetails[0]
                .setModificationOrdeletionTimesStamp(modificationOrdeletionTimesStamp);
        loadMetadataDetails[0].setLoadStartTime("01-04-2016 07:42:58");
        loadMetadataDetails[1] = new LoadMetadataDetails();
        loadMetadataDetails[1].setLoadName("1");
        loadMetadataDetails[1]
                .setLoadStatus(CarbonCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS);
        loadMetadataDetails[1].setTimestamp("01-04-2016 07:53:00");
        loadMetadataDetails[1].setLoadStartTime("01-04-2016 07:54:58");
        loadMetadataDetails[2] = new LoadMetadataDetails();
        loadMetadataDetails[2].setLoadName("2");
        loadMetadataDetails[2].setLoadStatus(CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS);
        loadMetadataDetails[2].setTimestamp("01-04-2016 07:58:00");
        loadMetadataDetails[2].setLoadStartTime("01-04-2016 07:59:58");
        Map<String, Long> loadNameAndModOrDelMapping =
                InMemoryLoadTableUtil.createLoadAndModificationTimeMappping(loadMetadataDetails);
        List<String> listLoadFolders = new ArrayList<String>();
        listLoadFolders.add("0");
        listLoadFolders.add("1");
        listLoadFolders.add("2");
        List<RestructureStore> slices = new ArrayList<RestructureStore>();
        String factTableName = "alldatatypescube_1";
        CarbonProperties.getInstance().addProperty(CarbonCommonConstants.MAX_QUERY_EXECUTION_TIME, "1");
        RestructureStore restructureStore = new RestructureStore("RS_", 0);
        CarbonDef.Schema schema = getSchema();
        InMemoryTable inMemoryTable =
                new InMemoryTable(schema, schema.cubes[0], null, factTableName, "",
                        carbonDateFormat.parse(modificationOrdeletionTimesStamp).getTime());
        inMemoryTable.setLoadName(CarbonCommonConstants.LOAD_FOLDER + "0");
        Map<String, List<InMemoryTable>> mapOfSlices = new HashMap<>();
        List<InMemoryTable> lisOfSlices = new ArrayList<>();
        lisOfSlices.add(inMemoryTable);
        mapOfSlices.put(factTableName, lisOfSlices);
        restructureStore.setSlices(mapOfSlices);
        slices.add(restructureStore);
        List<InMemoryTable> activeSlices = new ArrayList<>();
        restructureStore.getActiveSlices(activeSlices);
        Thread.sleep(1000);
        InMemoryLoadTableUtil.checkAndDeleteStaleSlices(slices, factTableName);
        List<InMemoryTable> activeSlice = new ArrayList<>();
        restructureStore.getActiveSlicesByTableName(activeSlice, factTableName);
        //After stale slices there should not be any slices
        Assert.assertTrue(activeSlice.isEmpty());

    }

    @Test public void getQuerySlices() throws Exception {
        LoadMetadataDetails[] loadMetadataDetails = new LoadMetadataDetails[3];
        loadMetadataDetails[0] = new LoadMetadataDetails();
        loadMetadataDetails[0].setLoadName("0");
        loadMetadataDetails[0].setLoadStatus(CarbonCommonConstants.MARKED_FOR_UPDATE);
        loadMetadataDetails[0].setTimestamp("01-04-2016 07:43:00");
        SimpleDateFormat carbonDateFormat =
                new SimpleDateFormat(CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT);
        String modificationOrdeletionTimesStamp = carbonDateFormat.format(new Date()).toString();
        loadMetadataDetails[0]
                .setModificationOrdeletionTimesStamp(modificationOrdeletionTimesStamp);
        loadMetadataDetails[0].setLoadStartTime("01-04-2016 07:42:58");
        loadMetadataDetails[1] = new LoadMetadataDetails();
        loadMetadataDetails[1].setLoadName("1");
        loadMetadataDetails[1]
                .setLoadStatus(CarbonCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS);
        loadMetadataDetails[1].setTimestamp("01-04-2016 07:53:00");
        loadMetadataDetails[1].setLoadStartTime("01-04-2016 07:54:58");
        loadMetadataDetails[2] = new LoadMetadataDetails();
        loadMetadataDetails[2].setLoadName("2");
        loadMetadataDetails[2].setLoadStatus(CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS);
        loadMetadataDetails[2].setTimestamp("01-04-2016 07:58:00");
        loadMetadataDetails[2].setLoadStartTime("01-04-2016 07:59:58");
        Map<String, Long> loadNameAndModOrDelMapping =
                InMemoryLoadTableUtil.createLoadAndModificationTimeMappping(loadMetadataDetails);
        List<String> listLoadFolders = new ArrayList<String>();
        listLoadFolders.add("0");
        listLoadFolders.add("1");
        listLoadFolders.add("2");
        List<RestructureStore> slices = new ArrayList<RestructureStore>();
        String factTableName = "alldatatypescube_1";

        RestructureStore restructureStore = new RestructureStore("RS_", 0);
        CarbonDef.Schema schema = getSchema();
        InMemoryTable inMemoryTable =
                new InMemoryTable(schema, schema.cubes[0], null, factTableName, "",
                        carbonDateFormat.parse(modificationOrdeletionTimesStamp).getTime());
        inMemoryTable.setLoadName(CarbonCommonConstants.LOAD_FOLDER + "0");
        Map<String, List<InMemoryTable>> mapOfSlices = new HashMap<>();
        List<InMemoryTable> lisOfSlices = new ArrayList<>();
        lisOfSlices.add(inMemoryTable);
        mapOfSlices.put(factTableName, lisOfSlices);
        restructureStore.setSlices(mapOfSlices);
        slices.add(restructureStore);
        List<InMemoryTable> activeSlices = new ArrayList<>();
        restructureStore.getActiveSlices(activeSlices);
        List<InMemoryTable> querySlices =
                InMemoryLoadTableUtil.getQuerySlices(activeSlices, loadNameAndModOrDelMapping);
        Assert.assertTrue(1 == querySlices.size());
    }

    private CarbonDef.Schema getSchema() {

        String schemaStr = "<Schema name=\"default\">\n"
                + "\t<Cube name=\"alldatatypescube_1\" visible=\"true\" cache=\"true\" enabled=\"true\" autoAggregationType=\"NONE\" mode=\"file\">\n"
                + "\t\t<Table name=\"alldatatypescube_1\">\n" + "\t\t</Table>\n"
                + "\t\t<Dimension type=\"StandardDimension\" visible=\"true\" noDictionary=\"false\" name=\"empno\">\n"
                + "\t\t\t<Hierarchy name=\"empno\" visible=\"true\" hasAll=\"true\" normalized=\"false\">\n"
                + "\t\t\t\t<Level name=\"empno\" visible=\"true\" columnIndex=\"-1\" keyOrdinal=\"-1\" levelCardinality=\"-1\" ordinalColumnIndex=\"-1\" nameColumnIndex=\"-1\" column=\"empno\" type=\"Integer\" uniqueMembers=\"false\" columnar=\"true\" levelType=\"Regular\" hideMemberIf=\"Never\" isParent=\"true\">\n"
                + "\t\t\t\t</Level>\n" + "\t\t\t</Hierarchy>\n" + "\t\t</Dimension>\n"
                + "\t\t<Measure name=\"salary\" column=\"salary\" datatype=\"Integer\" aggregator=\"sum\" visible=\"true\">\n"
                + "\t\t</Measure>\n" + "\t</Cube>\n" + "</Schema>";
        Parser xmlParser = null;
        CarbonDef.Schema schema = null;
        try {
            xmlParser = XOMUtil.createDefaultParser();
            ByteArrayInputStream baoi =
                    new ByteArrayInputStream(schemaStr.getBytes(Charset.defaultCharset()));
            schema = new CarbonDef.Schema(xmlParser.parse(baoi));
        } catch (XOMException e) {
            e.printStackTrace();
        }

        return schema;
    }
}