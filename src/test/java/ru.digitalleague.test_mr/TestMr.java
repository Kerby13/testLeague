package ru.digitalleague.test_mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Test;
import ru.digitalleague.test_mr.staging.Stage1.ActivityMapper;
import ru.digitalleague.test_mr.staging.Stage1.FirstStageReducer;
import ru.digitalleague.test_mr.staging.tools.IOHelper;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class TestMr {
    private MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mapReduceDriver;
    private IOHelper ioHelper = new IOHelper();
    private String resourcePath = String.join(File.separator, "src", "test", "resources");
    private String inputFilePath = String.join(File.separator, resourcePath, "cdr.csv");
    private String outputFilePath = String.join(File.separator, resourcePath, "shop_output.csv");

    @Test
    public void testMr() throws IOException {
        ActivityMapper mapper = new ActivityMapper();
        FirstStageReducer reducer = new FirstStageReducer();

        mapReduceDriver = new MapReduceDriver<>();

        mapReduceDriver.setMapper(mapper);
        mapReduceDriver.setReducer(reducer);

        mapReduceDriver.addAll(ioHelper.read(inputFilePath));
        List<Pair<Text, Text>> result = mapReduceDriver.run();

        ioHelper.write(outputFilePath, result);
    }
}
