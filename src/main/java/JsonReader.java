import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.CSVWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

public class JsonReader
{
	private static final Logger LOGGER = LoggerFactory.getLogger (JsonReader.class);

	private static final String SUBMAP = "HEALTH";

	private List<LinkedHashMap> healthResult = new ArrayList<LinkedHashMap>();
	private List<LinkedHashMap> nonHealthResult = new ArrayList<LinkedHashMap>();

	public String ReadFileContentsIntoString(String path) throws IOException {
		String file = path;
		BufferedReader reader = new BufferedReader(new FileReader(file));
		StringBuilder builder = new StringBuilder();
		String currentLine = reader.readLine();
		while (currentLine != null) {
			builder.append(currentLine);
			builder.append("n");
			currentLine = reader.readLine();
		}
		reader.close();

		return builder.toString ();
	}

	public String[] GetHeaders(String path) throws IOException {
		String jsonString = null;
		try {
			jsonString = ReadFileContentsIntoString (path);
		} catch (IOException e) {
			LOGGER.error("error reading file for the given path {}",path);
		}

		ObjectMapper mapper = new ObjectMapper();
		List<LinkedHashMap> result = mapper.readValue(jsonString, List.class);

		LinkedHashSet<String> headers = new LinkedHashSet<String>();

		for (int i = 0; i<result.size ();i++){

			LinkedHashMap<String,Object> map = (LinkedHashMap<String, Object>) result.get (i);
			LinkedHashMap<String,Object> healthMap = mapper.readValue ((String) map.get (SUBMAP),LinkedHashMap.class);
            healthResult.add(healthMap);
			map.remove(SUBMAP);
			nonHealthResult.add(map);
			headers.addAll(map.keySet());
			headers.addAll(healthMap.keySet());

		}

		return headers.stream().toArray(String[]::new);
	}

	public void writeFile(String [] header) {

		File outputFile = new File ("./src/main/resources/out.csv");
		FileWriter fileWriter = null;
		try {
			fileWriter = new FileWriter(outputFile);
			CSVWriter csvWriter = new CSVWriter(fileWriter);
			csvWriter.writeNext(header);
			int counter = 1;

			for (int i = 0; i<nonHealthResult.size();i++){
				List<Object> line = new ArrayList<>();
				LinkedHashMap<String,Object> map2 = healthResult.get(i);
				LinkedHashMap<String, Object> map1 = nonHealthResult.get(i);
                counter++;
				line.addAll(map1.values());
                LOGGER.info("writing line {}",counter);
				for(int index = 7;index<header.length;index++){
					Object value = map2.get(header[index]);

					if (value == null){
						line.add(" ");
					}
					else {
						line.add(value);
					}
				}
				String [] res = Arrays.stream(line.toArray()).map(Object::toString).toArray(String[]::new);
				System.out.println(Arrays.toString(res));
				csvWriter.writeNext(res);
			}
			csvWriter.close();
			LOGGER.info("file writing complete! total records {}",counter);
		} catch (IOException e) {
			LOGGER.error("can not write file",e.getCause());
		}

	}

	public static void main(String[] args) {
		JsonReader jsonReader = new JsonReader();
		String path = "src/main/resources/file.txt";
		try {
			String [] header = jsonReader.GetHeaders(path);
			jsonReader.writeFile(header);
		} catch (IOException e) {
			LOGGER.error("can not get the header of the file ",e.getCause());
		}

	}
}
