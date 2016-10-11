package org.apache.carbondata.processing.newflow;

import java.io.Serializable;

import org.apache.carbondata.core.carbon.metadata.blocklet.compressor.CompressionCodec;
import org.apache.carbondata.core.carbon.metadata.encoder.Encoding;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonColumn;

/**
 * Metadata class for each column of table.
 */
public class DataField implements Serializable {

  private CarbonColumn column;

  private CompressionCodec compressionCodec;

  public boolean hasDictionaryEncoding() {
    return column.hasEncoding(Encoding.DICTIONARY);
  }

  public CarbonColumn getColumn() {
    return column;
  }

  public void setColumn(CarbonColumn column) {
    this.column = column;
  }

  public CompressionCodec getCompressionCodec() {
    return compressionCodec;
  }

  public void setCompressionCodec(CompressionCodec compressionCodec) {
    this.compressionCodec = compressionCodec;
  }

}
