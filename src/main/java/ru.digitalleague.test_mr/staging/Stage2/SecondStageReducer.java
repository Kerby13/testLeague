package ru.digitalleague.test_mr.staging.Stage2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SecondStageReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        String name = "";
        String phone = "";
        for (Text t : values) {
            String[] parts = t.toString().split("    ");
            if (parts[0].equals("phone")) {
                phone = parts[1];
            } else if (parts[0].equals("name")) {
                name = parts[1];
            }
        }
        if (!phone.equals(""))
            context.write(new Text(phone), new Text(name));
    }
}
