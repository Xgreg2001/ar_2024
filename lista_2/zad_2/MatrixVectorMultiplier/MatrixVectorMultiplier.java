import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class MatrixVectorMultiplier {
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Użycie: MatrixVectorMultiplier <ścieżka macierzy> <ścieżka wektora> <ścieżka wyjściowa>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Mnożenie macierzy przez wektor");

        job.setJarByClass(MatrixVectorMultiplier.class);
        job.setMapperClass(MatrixMapper.class);
        job.setReducerClass(ProductReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

        // Dodanie pliku wektora do Distributed Cache
        job.addCacheFile(new Path(args[1]).toUri());

        // Ustawienie ścieżek wejściowych i wyjściowych
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

