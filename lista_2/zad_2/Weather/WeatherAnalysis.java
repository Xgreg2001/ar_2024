import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WeatherAnalysis {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Użycie: WeatherAnalysis <ścieżka wejściowa> <ścieżka wyjściowa> [rok]");
            System.exit(-1);
        }

        Configuration conf = new Configuration();

        // Jeśli podano rok jako trzeci argument, ustaw go w konfiguracji
        if (args.length == 3) {
            conf.set("targetYear", args[2]);
        }

        Job job = Job.getInstance(conf, "Analiza danych pogodowych");
        job.setJarByClass(WeatherAnalysis.class);
        job.setMapperClass(WeatherMapper.class);
        job.setReducerClass(WeatherReducer.class);

        // Ustawienie klas wyjściowych Mappera
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Ustawienie klas wyjściowych Reducera
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

