import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class MatrixMapper extends Mapper<Object, Text, IntWritable, DoubleWritable> {
    private HashMap<Integer, Double> vector = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Odczytanie wektora z Distributed Cache
        URI[] cacheFiles = context.getCacheFiles();
        if (cacheFiles != null && cacheFiles.length > 0) {
            String line;
            FileSystem fs = FileSystem.get(context.getConfiguration());
            Path getPath = new Path(cacheFiles[0].toString());
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(getPath)));
            try {
                while ((line = reader.readLine()) != null) {
                    String[] tokens = line.trim().split("\\s+");
                    int index = Integer.parseInt(tokens[0]);
                    double value = Double.parseDouble(tokens[1]);
                    vector.put(index, value);
                }
            } finally {
                reader.close();
            }
        }
    }

    private IntWritable outKey = new IntWritable();
    private DoubleWritable outValue = new DoubleWritable();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Format wejścia: indeks_wiersza indeks_kolumny wartość
        String[] tokens = value.toString().trim().split("\\s+");
        if (tokens.length == 3) {
            int row = Integer.parseInt(tokens[0]);
            int col = Integer.parseInt(tokens[1]);
            double matrixValue = Double.parseDouble(tokens[2]);

            // Pobierz wartość z wektora
            Double vectorValue = vector.get(col);
            if (vectorValue != null) {
                double partialProduct = matrixValue * vectorValue;
                outKey.set(row);
                outValue.set(partialProduct);
                context.write(outKey, outValue);
            }
        }
    }
}

