import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvertedIndexMapper extends Mapper<Object, Text, Text, Text> {

    private Text word = new Text();
    private Text documentId = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Pobierz nazwę pliku (identyfikator dokumentu)
        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
        documentId.set(fileName);
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Tokenizacja linii tekstu
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            // Normalizacja słów
            String token = itr.nextToken().replaceAll("\\W+", "").toLowerCase();
            if (!token.isEmpty()) {
                word.set(token);
                context.write(word, documentId);
            }
        }
    }
}

