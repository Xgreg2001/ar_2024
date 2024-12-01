import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndex {

    public static void main(String[] args) throws Exception {
        // Sprawdzenie argumentów
        if (args.length != 2) {
            System.err.println("Użycie: InvertedIndex <ścieżka wejściowa> <ścieżka wyjściowa>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Odwrotny Indeks");

        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(InvertedIndexMapper.class);
        job.setReducerClass(InvertedIndexReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Ustawienie ścieżek wejściowych i wyjściowych
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Uruchomienie zadania
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

