package Utils;


import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import java.io.*;

public class CSVFileUtils {

	public static int[][] readStructureFromCsv(String modelName) throws IOException {
		Reader csvReader = new FileReader("" + "Models/CSV/" + modelName + ".csv");
		Iterable<CSVRecord> records = CSVFormat.RFC4180.parse(csvReader);
		int[][] structure = new int[0][0];
		int iterTimes = 0;
		for (CSVRecord csvRecord:records) {
			if (iterTimes == 0) {
				int nrOfAttr = csvRecord.size();
				structure = new int[nrOfAttr][nrOfAttr];
			}
			for (int i=0;i<csvRecord.size();i++) {
				if (csvRecord.get(i).equals("1")) {
					structure[iterTimes][i] = 1;
				}
			}
			iterTimes++;
		}
		return structure;
	}
}
