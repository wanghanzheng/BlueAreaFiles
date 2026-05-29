import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.CellValue;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

public class MosExcelSplitter {
    private static final String[] SELECTED_COLUMNS = {
            "periodAvgRtt",
            "periodDropRate",
            "avgDownloadSpeed",
            "mos"
    };

    private static final String[] INTERVAL_SHEET_NAMES = {
            "区间1_1-1.5",
            "区间2_1.5-2",
            "区间3_2-2.5",
            "区间4_2.5-3",
            "区间5_3-3.5",
            "区间6_3.5-4",
            "区间7_4-4.5",
            "区间8_4.5-5"
    };

    public static void main(String[] args) throws Exception {
        // 在 IDEA 里直接修改这里，然后运行 main 方法即可。
        String inputFilePath = "D:/path/to/your/input.xlsx";

        // 如果要读取第一个 sheet，保持 null 即可；如果要指定 sheet，就写 sheet 名，例如 "总表"。
        String sourceSheetName = null;

        Path inputPath = Paths.get(inputFilePath).toAbsolutePath().normalize();
        if (!Files.exists(inputPath)) {
            throw new IllegalArgumentException("Input file does not exist: " + inputPath);
        }

        SplitResult result = splitWorkbook(inputPath, sourceSheetName);
        printResult(result);
    }

    private static SplitResult splitWorkbook(Path inputPath, String sourceSheetName) throws Exception {
        Path outputPath = buildDefaultOutputPath(inputPath);
        try (InputStream inputStream = Files.newInputStream(inputPath);
             Workbook workbook = WorkbookFactory.create(inputStream)) {
            DataFormatter formatter = new DataFormatter();
            FormulaEvaluator evaluator = workbook.getCreationHelper().createFormulaEvaluator();

            Sheet sourceSheet = findSourceSheet(workbook, sourceSheetName);
            HeaderInfo headerInfo = findHeaderInfo(sourceSheet, formatter);

            removeExistingIntervalSheets(workbook, sourceSheet);
            Sheet[] intervalSheets = createIntervalSheets(workbook);

            long[] counts = new long[INTERVAL_SHEET_NAMES.length];
            long discardedRows = 0L;

            for (int rowIndex = headerInfo.rowIndex + 1; rowIndex <= sourceSheet.getLastRowNum(); rowIndex++) {
                Row sourceRow = sourceSheet.getRow(rowIndex);
                if (sourceRow == null) {
                    continue;
                }

                Double mos = readNumericCell(sourceRow.getCell(headerInfo.columnIndexes.get("mos")), evaluator);
                int intervalIndex = getIntervalIndex(mos);
                if (intervalIndex < 0) {
                    discardedRows++;
                    continue;
                }

                Sheet targetSheet = intervalSheets[intervalIndex];
                Row targetRow = targetSheet.createRow((int) counts[intervalIndex] + 1);
                copySelectedCells(sourceRow, targetRow, headerInfo, evaluator);
                counts[intervalIndex]++;
            }

            autoSizeColumns(intervalSheets);
            writeWorkbook(workbook, inputPath, outputPath);

            return new SplitResult(sourceSheet.getSheetName(), outputPath, counts, discardedRows);
        }
    }

    private static Sheet findSourceSheet(Workbook workbook, String sourceSheetName) {
        if (sourceSheetName == null || sourceSheetName.trim().isEmpty()) {
            Sheet firstSheet = workbook.getNumberOfSheets() > 0 ? workbook.getSheetAt(0) : null;
            if (firstSheet == null) {
                throw new IllegalArgumentException("Workbook has no sheets.");
            }
            return firstSheet;
        }

        Sheet sheet = workbook.getSheet(sourceSheetName);
        if (sheet == null) {
            throw new IllegalArgumentException("Source sheet not found: " + sourceSheetName);
        }
        return sheet;
    }

    private static HeaderInfo findHeaderInfo(Sheet sheet, DataFormatter formatter) {
        for (int rowIndex = sheet.getFirstRowNum(); rowIndex <= sheet.getLastRowNum(); rowIndex++) {
            Row row = sheet.getRow(rowIndex);
            if (row == null) {
                continue;
            }

            Map<String, Integer> columnIndexes = findColumns(row, formatter);
            if (columnIndexes.size() == SELECTED_COLUMNS.length) {
                return new HeaderInfo(rowIndex, columnIndexes);
            }
        }

        throw new IllegalArgumentException("Could not find required columns in sheet: "
                + sheet.getSheetName() + ". Required columns: " + Arrays.toString(SELECTED_COLUMNS));
    }

    private static Map<String, Integer> findColumns(Row row, DataFormatter formatter) {
        Map<String, Integer> columnIndexes = new LinkedHashMap<>();
        for (Cell cell : row) {
            String value = formatter.formatCellValue(cell).trim();
            for (String columnName : SELECTED_COLUMNS) {
                if (columnName.equals(value) && !columnIndexes.containsKey(columnName)) {
                    columnIndexes.put(columnName, cell.getColumnIndex());
                    break;
                }
            }
        }
        return columnIndexes;
    }

    private static void removeExistingIntervalSheets(Workbook workbook, Sheet sourceSheet) {
        for (String sheetName : INTERVAL_SHEET_NAMES) {
            int sheetIndex = workbook.getSheetIndex(sheetName);
            if (sheetIndex < 0) {
                continue;
            }
            if (sheetIndex == workbook.getSheetIndex(sourceSheet)) {
                throw new IllegalArgumentException("Source sheet name conflicts with interval sheet: " + sheetName);
            }
            workbook.removeSheetAt(sheetIndex);
        }
    }

    private static Sheet[] createIntervalSheets(Workbook workbook) {
        Sheet[] sheets = new Sheet[INTERVAL_SHEET_NAMES.length];
        for (int i = 0; i < INTERVAL_SHEET_NAMES.length; i++) {
            Sheet sheet = workbook.createSheet(INTERVAL_SHEET_NAMES[i]);
            Row headerRow = sheet.createRow(0);
            for (int columnIndex = 0; columnIndex < SELECTED_COLUMNS.length; columnIndex++) {
                headerRow.createCell(columnIndex, CellType.STRING).setCellValue(SELECTED_COLUMNS[columnIndex]);
            }
            sheets[i] = sheet;
        }
        return sheets;
    }

    private static void copySelectedCells(Row sourceRow, Row targetRow, HeaderInfo headerInfo,
                                          FormulaEvaluator evaluator) {
        for (int targetColumnIndex = 0; targetColumnIndex < SELECTED_COLUMNS.length; targetColumnIndex++) {
            String columnName = SELECTED_COLUMNS[targetColumnIndex];
            Cell sourceCell = sourceRow.getCell(headerInfo.columnIndexes.get(columnName));
            Cell targetCell = targetRow.createCell(targetColumnIndex);
            copyCellValue(sourceCell, targetCell, evaluator);
        }
    }

    private static void copyCellValue(Cell sourceCell, Cell targetCell, FormulaEvaluator evaluator) {
        if (sourceCell == null) {
            targetCell.setBlank();
            return;
        }

        targetCell.setCellStyle(sourceCell.getCellStyle());
        CellType cellType = sourceCell.getCellType();
        if (cellType == CellType.FORMULA) {
            copyEvaluatedFormulaValue(sourceCell, targetCell, evaluator);
            return;
        }

        switch (cellType) {
            case STRING:
                targetCell.setCellValue(sourceCell.getStringCellValue());
                break;
            case NUMERIC:
                if (DateUtil.isCellDateFormatted(sourceCell)) {
                    targetCell.setCellValue(sourceCell.getDateCellValue());
                } else {
                    targetCell.setCellValue(sourceCell.getNumericCellValue());
                }
                break;
            case BOOLEAN:
                targetCell.setCellValue(sourceCell.getBooleanCellValue());
                break;
            case ERROR:
                targetCell.setCellErrorValue(sourceCell.getErrorCellValue());
                break;
            case BLANK:
            case _NONE:
            default:
                targetCell.setBlank();
                break;
        }
    }

    private static void copyEvaluatedFormulaValue(Cell sourceCell, Cell targetCell, FormulaEvaluator evaluator) {
        CellValue evaluatedValue = evaluator.evaluate(sourceCell);
        if (evaluatedValue == null) {
            targetCell.setBlank();
            return;
        }

        switch (evaluatedValue.getCellType()) {
            case STRING:
                targetCell.setCellValue(evaluatedValue.getStringValue());
                break;
            case NUMERIC:
                if (DateUtil.isCellDateFormatted(sourceCell)) {
                    targetCell.setCellValue(DateUtil.getJavaDate(evaluatedValue.getNumberValue()));
                } else {
                    targetCell.setCellValue(evaluatedValue.getNumberValue());
                }
                break;
            case BOOLEAN:
                targetCell.setCellValue(evaluatedValue.getBooleanValue());
                break;
            case ERROR:
                targetCell.setCellErrorValue(evaluatedValue.getErrorValue());
                break;
            case BLANK:
            case _NONE:
            default:
                targetCell.setBlank();
                break;
        }
    }

    private static Double readNumericCell(Cell cell, FormulaEvaluator evaluator) {
        if (cell == null) {
            return null;
        }

        CellType cellType = cell.getCellType();
        if (cellType == CellType.FORMULA) {
            CellValue evaluatedValue = evaluator.evaluate(cell);
            if (evaluatedValue == null) {
                return null;
            }
            if (evaluatedValue.getCellType() == CellType.NUMERIC) {
                return evaluatedValue.getNumberValue();
            }
            if (evaluatedValue.getCellType() == CellType.STRING) {
                return parseDouble(evaluatedValue.getStringValue());
            }
            return null;
        }

        if (cellType == CellType.NUMERIC) {
            return cell.getNumericCellValue();
        }
        if (cellType == CellType.STRING) {
            return parseDouble(cell.getStringCellValue());
        }
        return null;
    }

    private static Double parseDouble(String value) {
        if (value == null) {
            return null;
        }

        String trimmedValue = value.trim().replace(",", "");
        if (trimmedValue.isEmpty()) {
            return null;
        }

        try {
            return Double.parseDouble(trimmedValue);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static int getIntervalIndex(Double mos) {
        if (mos == null || mos.isNaN() || mos.isInfinite() || mos < 1.0D || mos > 5.0D) {
            return -1;
        }
        if (mos == 5.0D) {
            return INTERVAL_SHEET_NAMES.length - 1;
        }

        int index = (int) Math.floor((mos - 1.0D) / 0.5D);
        return index >= 0 && index < INTERVAL_SHEET_NAMES.length ? index : -1;
    }

    private static void autoSizeColumns(Sheet[] sheets) {
        for (Sheet sheet : sheets) {
            for (int columnIndex = 0; columnIndex < SELECTED_COLUMNS.length; columnIndex++) {
                sheet.autoSizeColumn(columnIndex);
            }
        }
    }

    private static void writeWorkbook(Workbook workbook, Path inputPath, Path outputPath) throws IOException {
        Path outputParent = outputPath.getParent();
        if (outputParent != null) {
            Files.createDirectories(outputParent);
        }

        if (inputPath.equals(outputPath)) {
            Path tempPath = Files.createTempFile(outputParent, "mos-split-", ".tmp");
            try (OutputStream outputStream = Files.newOutputStream(tempPath)) {
                workbook.write(outputStream);
            }
            Files.move(tempPath, outputPath, StandardCopyOption.REPLACE_EXISTING);
            return;
        }

        try (OutputStream outputStream = Files.newOutputStream(outputPath)) {
            workbook.write(outputStream);
        }
    }

    private static Path buildDefaultOutputPath(Path inputPath) {
        String fileName = inputPath.getFileName().toString();
        String lowerName = fileName.toLowerCase();
        String outputFileName;

        if (lowerName.endsWith(".xlsx")) {
            outputFileName = fileName.substring(0, fileName.length() - ".xlsx".length()) + "_mos_split.xlsx";
        } else if (lowerName.endsWith(".xls")) {
            outputFileName = fileName.substring(0, fileName.length() - ".xls".length()) + "_mos_split.xls";
        } else {
            outputFileName = fileName + "_mos_split.xlsx";
        }

        Path parent = inputPath.getParent();
        return parent == null ? Paths.get(outputFileName).toAbsolutePath().normalize() : parent.resolve(outputFileName);
    }

    private static void printResult(SplitResult result) {
        System.out.println("Source sheet: " + result.sourceSheetName);
        for (int i = 0; i < INTERVAL_SHEET_NAMES.length; i++) {
            System.out.println(INTERVAL_SHEET_NAMES[i] + ": " + result.counts[i] + " rows");
        }
        System.out.println("Discarded rows: " + result.discardedRows);
        System.out.println("Output file: " + result.outputPath);
    }

    private static class HeaderInfo {
        private final int rowIndex;
        private final Map<String, Integer> columnIndexes;

        private HeaderInfo(int rowIndex, Map<String, Integer> columnIndexes) {
            this.rowIndex = rowIndex;
            this.columnIndexes = columnIndexes;
        }
    }

    private static class SplitResult {
        private final String sourceSheetName;
        private final Path outputPath;
        private final long[] counts;
        private final long discardedRows;

        private SplitResult(String sourceSheetName, Path outputPath, long[] counts, long discardedRows) {
            this.sourceSheetName = sourceSheetName;
            this.outputPath = outputPath;
            this.counts = counts;
            this.discardedRows = discardedRows;
        }
    }
}
