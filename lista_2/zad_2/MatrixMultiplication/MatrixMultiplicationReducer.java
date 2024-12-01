import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;

public class MatrixMultiplicationReducer extends Reducer<IntWritable, Text, Text, DoubleWritable> {

    private int n;
    private DoubleWritable result = new DoubleWritable();
    private Text outKey = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        n = context.getConfiguration().getInt("n", 0);
    }

    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        HashMap<Integer, Double> aMap = new HashMap<>();
        HashMap<Integer, Double> bMap = new HashMap<>();

        for (Text val : values) {
            String[] tokens = val.toString().split(",");
            String matrixName = tokens[0];
            int index = Integer.parseInt(tokens[1]);
            double value = Double.parseDouble(tokens[2]);

            if (matrixName.equals("A")) {
                aMap.put(index, value);
            } else if (matrixName.equals("B")) {
                bMap.put(index, value);
            }
        }

        // Compute the sum over all k: A(i,k) * B(k,j)
        double sum = 0.0;
        for (int k = 0; k < n; k++) {
            double aVal = aMap.getOrDefault(k, 0.0);
            double bVal = bMap.getOrDefault(k, 0.0);
            sum += aVal * bVal;
        }

        if (sum != 0.0) {
            int i = key.get() / n;
            int j = key.get() % n;
            outKey.set(i + "," + j);
            result.set(sum);
            context.write(outKey, result);
        }
    }
}
