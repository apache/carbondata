package org.apache.carbondata.core.indexstore.blockletindex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.CarbonFileFilter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.events.ChangeEvent;
import org.apache.carbondata.core.indexstore.Blocklet;
import org.apache.carbondata.core.indexstore.DataMap;
import org.apache.carbondata.core.indexstore.DataMapDistributable;
import org.apache.carbondata.core.indexstore.DataMapWriter;
import org.apache.carbondata.core.indexstore.AbstractTableDataMap;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;

/**
 * Created by root1 on 16/6/17.
 */
public class BlockletTableMap extends AbstractTableDataMap {

  private String dataMapName;

  private AbsoluteTableIdentifier identifier;

  private Map<String, List<DataMap>> map = new HashMap<>();

  @Override public void init(AbsoluteTableIdentifier identifier, String dataMapName) {
    this.identifier = identifier;
    this.dataMapName = dataMapName;
  }

  @Override public DataMapWriter getMetaDataWriter() {
    return null;
  }

  @Override
  public DataMapWriter getDataMapWriter(AbsoluteTableIdentifier identifier, String segmentId) {
    return null;
  }

  @Override protected List<DataMap> getDataMaps(String segmentId) {
    List<DataMap> dataMaps = map.get(segmentId);
    if (dataMaps == null) {
      dataMaps = new ArrayList<>();
      String path = identifier.getTablePath() + "/Part0/Segment_" + segmentId;
      FileFactory.FileType fileType = FileFactory.getFileType(path);
      CarbonFile carbonFile = FileFactory.getCarbonFile(path, fileType);
      CarbonFile[] listFiles = carbonFile.listFiles(new CarbonFileFilter() {
        @Override public boolean accept(CarbonFile file) {
          return file.getName().endsWith(".carbonindex");
        }
      });
      for (int i = 0; i < listFiles.length; i++) {
        BlockletDataMap dataMap = new BlockletDataMap();
        dataMap.init(listFiles[i].getAbsolutePath());
        dataMaps.add(dataMap);
      }
    }
    return dataMaps;
  }

  @Override public List<DataMapDistributable> toDistributable(List<String> segmentIds) {
    return null;
  }

  @Override protected DataMap getDataMap(DataMapDistributable distributable) {
    return null;
  }

  @Override public boolean isFiltersSupported(FilterResolverIntf filterExp) {
    return false;
  }

  @Override public void clear() {

  }

  @Override public void fireEvent(ChangeEvent event) {

  }
}
