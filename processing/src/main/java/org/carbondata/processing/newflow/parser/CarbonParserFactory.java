package org.carbondata.processing.newflow.parser;

import java.util.List;

import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;

import org.carbondata.processing.newflow.parser.impl.ArrayParserImpl;
import org.carbondata.processing.newflow.parser.impl.PrimitiveParserImpl;
import org.carbondata.processing.newflow.parser.impl.StructParserImpl;

public class CarbonParserFactory {

  public static GenericParser createParser(CarbonColumn carbonColumn, char[] complexDelimiters) {
    return createParser(carbonColumn, complexDelimiters, 0);
  }

  private static GenericParser createParser(CarbonColumn carbonColumn, char[] complexDelimiters,
      int counter) {
    switch (carbonColumn.getDataType()) {
      case ARRAY:
        List<CarbonDimension> listOfChildDimensions =
            ((CarbonDimension) carbonColumn).getListOfChildDimensions();
        ArrayParserImpl arrayParser = new ArrayParserImpl(complexDelimiters[counter]);
        for (CarbonDimension dimension : listOfChildDimensions) {
          arrayParser.addChildren(createParser(dimension, complexDelimiters, counter + 1));
        }
        return arrayParser;
      case STRUCT:
        List<CarbonDimension> dimensions =
            ((CarbonDimension) carbonColumn).getListOfChildDimensions();
        StructParserImpl parser = new StructParserImpl(complexDelimiters[counter]);
        for (CarbonDimension dimension : dimensions) {
          parser.addChildren(createParser(dimension, complexDelimiters, counter + 1));
        }
        return parser;
      default:
        return new PrimitiveParserImpl();
    }
  }
}
