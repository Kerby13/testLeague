package ru.digitalleague.test_mr.staging.Stage2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PhoneMapper extends Mapper<NullWritable, Text, Text, Text> {
    private String INPUT_DELIMITER = "\u0001";
    private String STRING_DELIMITER = "    ";
    private int BAN_KEY = 0;
    private int PHONE_NUMBER = 1;
    private static String FILE_TAG = "phone";

    int MIN_LENGTH = 2;

    Text KEY = new Text();
    Text VALUE = new Text();

    @Override
    protected void map(NullWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] splittedValue = value.toString().split(INPUT_DELIMITER);
        if (splittedValue.length >= MIN_LENGTH) {
            String ban = splittedValue[BAN_KEY];
            String phone_number = "";
            if (!splittedValue[PHONE_NUMBER].equals(""))
                phone_number = splittedValue[PHONE_NUMBER];

            KEY.set(ban);
            VALUE.set(String.join(STRING_DELIMITER, FILE_TAG, phone_number));

            context.write(KEY, VALUE);
        }
    }
}
