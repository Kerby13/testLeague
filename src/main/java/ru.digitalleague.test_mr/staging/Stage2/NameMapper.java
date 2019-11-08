package ru.digitalleague.test_mr.staging.Stage2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class NameMapper extends Mapper<Text, Text, Text, Text> {
    private String INPUT_DELIMITER = "\u0001";
    private String STRING_DELIMITER = "    ";
    private int BAN_KEY = 0;
    private int FULL_NAME = 14;
    private static String FILE_TAG = "name";

    int MIN_LENGTH = 15;

    Text KEY = new Text();
    Text VALUE = new Text();

    @Override
    protected void map(Text key, Text value, Mapper.Context context)
            throws IOException, InterruptedException {
        String[] splittedValue = value.toString().split(INPUT_DELIMITER);
        if (splittedValue.length >= MIN_LENGTH) {
            String ban = splittedValue[BAN_KEY];
            String name = splittedValue[FULL_NAME];

            KEY.set(ban);
            VALUE.set(String.join(STRING_DELIMITER, FILE_TAG, name));

        }
        context.write(KEY, VALUE);
    }
}
