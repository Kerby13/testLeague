package ru.digitalleague.test_mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.MultipleInputsMapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Test;
import ru.digitalleague.test_mr.staging.SecondStage;
import ru.digitalleague.test_mr.staging.ThirdStage;
import ru.digitalleague.test_mr.staging.tools.IOHelper2;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class SecondStageTest {
    private MapReduceDriver<LongWritable, Text, Text, Text, LongWritable, Text> mapReduceDriver;

    private MultipleInputsMapReduceDriver<Text, Text, Text, Text> mrd;
    private MultipleInputsMapReduceDriver<Text, Text, Text, Text> mrdStageJoin;

    private IOHelper2 ioHelper = new IOHelper2();
    private String resourcePath = String.join(File.separator, "src", "test", "resources");
    private String inputFilePath1 = String.join(File.separator, resourcePath, "DIM_BAN.csv");
    private String inputFilePath2 = String.join(File.separator, resourcePath, "DIM_SUBSCRIBER.csv");
    private String firstStgFilePath = String.join(File.separator, resourcePath, "shop_output.csv");
    private String secondStgFilePath = String.join(File.separator, resourcePath, "shop_output2.csv");
    private String outputFilePath = String.join(File.separator, resourcePath, "shop_output2.csv");
    private String joinedOutputFilePath = String.join(File.separator, resourcePath, "shop_output3.csv");

    @Test
    public void testMr() throws IOException {
        SecondStage.NameMapper mapper1 = new SecondStage.NameMapper();
        SecondStage.SubscriberMapper mapper2 = new SecondStage.SubscriberMapper();
        SecondStage.JoinReducer reducer = new SecondStage.JoinReducer();

        /*mrd = new MultipleInputsMapReduceDriver<>();
        mrd.addMapper(mapper1);
        mrd.addMapper(mapper2);*/
        mrd = MultipleInputsMapReduceDriver.newMultipleInputMapReduceDriver(reducer).withMapper(mapper1).withMapper(mapper2);

        mrd.addAll(mapper1, ioHelper.read(inputFilePath1));
        mrd.addAll(mapper2, ioHelper.read(inputFilePath2));
        List<Pair<Text, Text>> result = mrd.run();

        ioHelper.write(outputFilePath, result);
//------------------------------------------------
        ThirdStage.FirstStageMapper joinMapper1 = new ThirdStage.FirstStageMapper();
        ThirdStage.SecondStageMapper joinMapper2 = new ThirdStage.SecondStageMapper();
        ThirdStage.StageJoinReducer joinReducer = new ThirdStage.StageJoinReducer();

        mrdStageJoin = MultipleInputsMapReduceDriver.newMultipleInputMapReduceDriver(joinReducer).withMapper(joinMapper1).withMapper(joinMapper2);

        mrdStageJoin.addAll(joinMapper1, ioHelper.read(firstStgFilePath));
        mrdStageJoin.addAll(joinMapper2, ioHelper.read(secondStgFilePath));
        List<Pair<Text, Text>> joinedResult = mrdStageJoin.run();

        ioHelper.write(joinedOutputFilePath, joinedResult);

    }
}
