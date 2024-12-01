import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;

public class MatrixMultiplication {
    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            System.err.println(
                    "Usage: MatrixMultiplication <input path> <output path> <n> <number of rows in A> <number of columns in B>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        // n is the shared dimension
        conf.setInt("n", Integer.parseInt(args[2]));
        // Number of rows in A
        conf.setInt("m", Integer.parseInt(args[3]));
        // Number of columns in B
        conf.setInt("p", Integer.parseInt(args[4]));

        Job job = Job.getInstance(conf, "Matrix Multiplication");
        job.setJarByClass(MatrixMultiplication.class);
        job.setMapperClass(MatrixMultiplicationMapper.class);
        job.setReducerClass(MatrixMultiplicationReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

