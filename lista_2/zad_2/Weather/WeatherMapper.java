import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WeatherMapper extends Mapper<Object, Text, Text, Text> {

    private Text monthKey = new Text();
    private Text dataValue = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Rozdzielenie linii na pola
        String[] fields = value.toString().split(",");
        if (fields.length >= 4) {
            try {
                // Pobieranie danych z kolumn
                String date = fields[0];
                double temperature = Double.parseDouble(fields[1]);
                double precipitation = Double.parseDouble(fields[2]);
                double humidity = Double.parseDouble(fields[3]);

                // Wyciągnięcie miesiąca i roku z daty (format YYYY-MM-DD)
                String[] dateParts = date.split("-");
                if (dateParts.length == 3) {
                    String month = dateParts[1]; // Miesiąc
                    String year = dateParts[0];  // Rok

                    // Ustawienie roku, jeśli chcesz filtrować dane dla konkretnego roku
                    String targetYear = context.getConfiguration().get("targetYear");

                    if (targetYear == null || targetYear.equals(year)) {
                        monthKey.set(month);
                        // Tworzymy wartość zawierającą temperaturę, opady i wilgotność
                        String data = temperature + "," + precipitation + "," + humidity;
                        dataValue.set(data);
                        context.write(monthKey, dataValue);
                    }
                }
            } catch (NumberFormatException e) {
                // Ignoruj błędnie sformatowane rekordy
            }
        }
    }
}

