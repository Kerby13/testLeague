package ru.digitalleague.test_mr.staging.Stage1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Arrays;

public class FirstStageReducer extends Reducer<Text, Text, Text, Text> {
    String OUTPUT_DELIMITER = ";";

    Text KEY = new Text();
    Text VALUE = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        int[] array = {0, 0, 0, 0, 0, 0, 0, 0};
        String[] splittedValue;
        for (Text value : values) {
            splittedValue = value.toString().split(";");

            for (int i = 0; i < splittedValue.length; i++) {
                if (!splittedValue[i].equals(""))
                    array[i] += Integer.parseInt(splittedValue[i]);
            }
        }

        String[] splittedKey = key.toString().split(";");

        if (splittedKey.length == 2) {
            VALUE.set(new Text(
                    String.join(OUTPUT_DELIMITER, splittedKey[1], Arrays.toString(array)
                            .replaceAll("\\s+", "")
                            .replaceAll("\\[", "")
                            .replaceAll("\\]", "")
                            .replaceAll(",", ";"))));
            KEY.set(splittedKey[0].replaceAll("\\s+", "").concat(";"));
            context.write(KEY, VALUE);
        }
    }
}
