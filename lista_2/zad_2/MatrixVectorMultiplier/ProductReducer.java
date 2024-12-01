import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class ProductReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
    private DoubleWritable result = new DoubleWritable();

    @Override
    public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
        double sum = 0.0;
        for (DoubleWritable val : values) {
            sum += val.get();
        }
        result.set(sum);
        context.write(key, result);
    }
}

