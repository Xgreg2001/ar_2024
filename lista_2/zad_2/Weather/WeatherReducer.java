import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WeatherReducer extends Reducer<Text, Text, Text, Text> {

    private Text resultValue = new Text();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        double sumTemp = 0.0, sumPrec = 0.0, sumHum = 0.0;
        double minTemp = Double.MAX_VALUE, maxTemp = Double.MIN_VALUE;
        double minPrec = Double.MAX_VALUE, maxPrec = Double.MIN_VALUE;
        double minHum = Double.MAX_VALUE, maxHum = Double.MIN_VALUE;
        int count = 0;

        for (Text val : values) {
            String[] data = val.toString().split(",");
            if (data.length == 3) {
                try {
                    double temp = Double.parseDouble(data[0]);
                    double prec = Double.parseDouble(data[1]);
                    double hum = Double.parseDouble(data[2]);

                    // Temperatury
                    sumTemp += temp;
                    if (temp < minTemp) minTemp = temp;
                    if (temp > maxTemp) maxTemp = temp;

                    // Opady
                    sumPrec += prec;
                    if (prec < minPrec) minPrec = prec;
                    if (prec > maxPrec) maxPrec = prec;

                    // Wilgotność
                    sumHum += hum;
                    if (hum < minHum) minHum = hum;
                    if (hum > maxHum) maxHum = hum;

                    count++;
                } catch (NumberFormatException e) {
                    // Ignoruj błędnie sformatowane dane
                }
            }
        }

        if (count > 0) {
            double avgTemp = sumTemp / count;
            double avgPrec = sumPrec / count;
            double avgHum = sumHum / count;

            String result = String.format(
                "Temp -> Avg: %.2f, Min: %.2f, Max: %.2f; " +
                "Prec -> Avg: %.2f, Min: %.2f, Max: %.2f; " +
                "Hum -> Avg: %.2f, Min: %.2f, Max: %.2f",
                avgTemp, minTemp, maxTemp,
                avgPrec, minPrec, maxPrec,
                avgHum, minHum, maxHum
            );

            resultValue.set(result);
            context.write(key, resultValue);
        }
    }
}

