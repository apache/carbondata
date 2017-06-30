package org.apache.carbondata.examples;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;

public class ExcelFeed {


  /**
   * this method reads data from an excel file named fileName from the sheet named sheetName
   */
  public static ArrayList<String[]> inputFeed(String fileName, String sheetName, int columnSize) throws IOException, NullPointerException, NoSuchElementException {
    System.out.println("Reading sheet: " + sheetName + " from file: " + fileName);
    ArrayList<String[]> excelData = new ArrayList<String[]>();
    FileInputStream fileInputStream = new FileInputStream(new File(fileName));
    HSSFWorkbook hssfWorkbook = new HSSFWorkbook(fileInputStream);
    HSSFSheet sheet = hssfWorkbook.getSheet(sheetName);
    if (null != sheet) {
      Iterator<Row> rowIterator = sheet.rowIterator();
      rowIterator.next();
      while (rowIterator.hasNext()) {
        Row next = rowIterator.next();
        String[] cells = new String[columnSize];
        for (int i = 0; i < columnSize; i++) {
          Cell cell = next.getCell(i);
          if (null == cell && i == 0) {
            return excelData;
          } else {
            if (null != cell) {
              int cellType = cell.getCellType();
              String stringCellValue = null;
              switch (cellType) {
                case 0:
                  stringCellValue = "" + (int) cell.getNumericCellValue();
                  break;
                case 1:
                  stringCellValue = cell.getStringCellValue();
                  break;
                default:
                  break;
              }
              cells[i] = stringCellValue;
            }
          }
        }
        Cell cell = next.getCell(columnSize);
        if (cell.getStringCellValue().equalsIgnoreCase("Yes")) excelData.add(cells);
      }
      fileInputStream.close();
    }
    return excelData;
  }

  /**
   * @param fileName Excel file Name
   * @param startsWith File Prefix
   * @return SheetNames
   * @throws IOException
   */
  public static List<String> getAllSheets(String fileName, String startsWith) throws IOException {
    List<String> sheetNames = new ArrayList<String>();
    FileInputStream fileInputStream = new FileInputStream(new File(fileName));
    HSSFWorkbook hssfWorkbook = new HSSFWorkbook(fileInputStream);
    int numberOfSheets = hssfWorkbook.getNumberOfSheets();
    for (int i = 0; i < numberOfSheets; i++) {
      String sheetName = hssfWorkbook.getSheetName(i);
      if (sheetName.startsWith(startsWith)) {
        sheetNames.add(sheetName);
      }
    }
    return sheetNames;
  }

  /**
   * @param fileName Excel file Name
   * @return SheetNames
   * @throws IOException
   */
  public static List<String> getAllSheets(String fileName) throws IOException {
    List<String> sheetNames = new ArrayList<String>();
    FileInputStream fileInputStream = new FileInputStream(new File(fileName));
    HSSFWorkbook hssfWorkbook = new HSSFWorkbook(fileInputStream);
    int numberOfSheets = hssfWorkbook.getNumberOfSheets();
    for (int i = 0; i < numberOfSheets; i++) {
      String sheetName = hssfWorkbook.getSheetName(i);
      sheetNames.add(sheetName);
    }
    return sheetNames;
  }

  /**
   * @param fileName
   * @param startsWith
   * @param columnSize
   * @return
   * @throws IOException
   * @throws NullPointerException
   */
  public static ArrayList<String[]> inputFeedFromMultipleSheets(String fileName, String startsWith,
      int columnSize) throws IOException, NullPointerException {
    ArrayList<String[]> result = new ArrayList<String[]>();
    List<String> sheetNames = getAllSheets(fileName, startsWith);
    System.out.println("Sheets: " + sheetNames);
    for (String sheetName : sheetNames) {
      result.addAll(inputFeed(fileName, sheetName, columnSize));
    }
    return result;
  }

  public static ArrayList<String[]> inputfeed(String filename, String sheetname, int col)
      throws IOException {
    ArrayList<String[]> arrayList = new ArrayList<String[]>();
    FileInputStream fileinputStream = new FileInputStream(new File(filename));
    HSSFWorkbook hssfWorkbook = new HSSFWorkbook(fileinputStream);
    HSSFSheet sheet = hssfWorkbook.getSheet(sheetname);
    if (null != sheet) {
      Iterator<Row> rowIterator = sheet.rowIterator();
      rowIterator.next();
      int columnSize = col;
      while (rowIterator.hasNext()) {
        Row next = rowIterator.next();
        String[] cells = new String[columnSize];
        for (int i = 0; i < columnSize; i++) {
          Cell cell = next.getCell(i);
          if (null == cell && i == 0) {
            return arrayList;
          } else {
            if (null != cell) {
              int cellType = cell.getCellType();
              String stringCellValue = null;
              switch (cellType) {
                case 0:
                  stringCellValue = "" + (int) cell.getNumericCellValue();
                  break;
                case 1:
                  stringCellValue = cell.getStringCellValue();
                  break;
                default:
                  break;
              }
              cells[i] = stringCellValue;
            }
          }
        }
        Cell cell = next.getCell(col);
        if (cell.getStringCellValue().equalsIgnoreCase("Yes")) arrayList.add(cells);
      }
      fileinputStream.close();
    }
    return arrayList;
  }

}
