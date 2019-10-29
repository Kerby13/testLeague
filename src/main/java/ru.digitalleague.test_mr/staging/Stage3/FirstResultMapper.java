package ru.digitalleague.test_mr.staging.Stage3;

import org.apache.commons.math3.analysis.function.Min;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FirstResultMapper extends Mapper<LongWritable, Text, Text, Text> {
    private String INPUT_DELIMITER = ";";
    private String STRING_DELIMITER = "    ";
    private int PHONE = 0;
    private static String FILE_TAG = "1st";

    int MIN_LENGTH = 2;

    Text KEY = new Text();
    Text VALUE = new Text();

    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context)
            throws IOException, InterruptedException {
        String[] splittedValue = value.toString().split(INPUT_DELIMITER);
        if (splittedValue.length >= MIN_LENGTH) {
            String phone = splittedValue[PHONE];
            String other = value.toString();//.replace(value.toString().substring(0, 11), "");

            KEY.set(phone);
            VALUE.set(String.join(STRING_DELIMITER, FILE_TAG, other));

            context.write(KEY, VALUE);
        }
    }
}
