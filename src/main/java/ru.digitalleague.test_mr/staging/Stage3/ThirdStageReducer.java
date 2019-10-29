package ru.digitalleague.test_mr.staging.Stage3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ThirdStageReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        String k = "";
        String v = "";
        for (Text t : values) {
            String[] parts = t.toString().split("    ");
            if (parts[0].equals("1st")) {
                k = parts[1];
            } else if (parts[0].equals("2nd")) {
                v = parts[1];
            }
        }
        if (!k.equals(""))
            if (v.equals(""))
                context.write(new Text(k), new Text("NULL"));
            else
                context.write(new Text(k), new Text(v));
    }
}
