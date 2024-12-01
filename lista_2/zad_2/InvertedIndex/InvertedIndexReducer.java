import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {

    private Text docIdList = new Text();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Zbiór unikalnych identyfikatorów dokumentów
        HashSet<String> docIds = new HashSet<>();
        for (Text val : values) {
            docIds.add(val.toString());
        }

        // Tworzenie listy identyfikatorów dokumentów
        StringBuilder sb = new StringBuilder();
        for (String docId : docIds) {
            sb.append(docId);
            sb.append(", ");
        }

        // Usunięcie ostatniego przecinka i spacji
        if (sb.length() > 0) {
            sb.setLength(sb.length() - 2);
        }

        docIdList.set(sb.toString());
        context.write(key, docIdList);
    }
}

