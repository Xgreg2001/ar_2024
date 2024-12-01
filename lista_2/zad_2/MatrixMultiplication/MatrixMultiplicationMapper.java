import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;

public class MatrixMultiplicationMapper extends Mapper<Object, Text, IntWritable, Text> {

    private int n; // Number of columns in A or number of rows in B

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Get the value of n from the configuration
        n = context.getConfiguration().getInt("n", 0);
    }

    private IntWritable outKey = new IntWritable();
    private Text outValue = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Input format: matrixName rowIndex columnIndex value
        String[] tokens = value.toString().split("\\s+");
        if (tokens.length != 4) {
            // Invalid input line
            return;
        }

        String matrixName = tokens[0];
        int i = Integer.parseInt(tokens[1]);
        int j = Integer.parseInt(tokens[2]);
        double val = Double.parseDouble(tokens[3]);

        if (matrixName.equals("A")) {
            // For each element in A, emit (i, "A,j,value")
            for (int k = 0; k < n; k++) {
                outKey.set(i * n + k); // Key represents position (i,k) in C
                outValue.set("A," + j + "," + val);
                context.write(outKey, outValue);
            }
        } else if (matrixName.equals("B")) {
            // For each element in B, emit (i, "B,j,value")
            for (int k = 0; k < n; k++) {
                outKey.set(k * n + j); // Key represents position (k,j) in C
                outValue.set("B," + i + "," + val);
                context.write(outKey, outValue);
            }
        }
    }
}
