package ru.digitalleague.test_mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.MultipleInputsMapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Test;
import ru.digitalleague.test_mr.staging.FirstStage;
import ru.digitalleague.test_mr.staging.SecondStage;
import ru.digitalleague.test_mr.staging.ThirdStage;
import ru.digitalleague.test_mr.staging.tools.IOHelper;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class SecondStageTest {
    private MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mrd1st;

    //private MultipleInputsMapReduceDriver<Text, Text, Text, Text> mrd1st;
    private MultipleInputsMapReduceDriver<Text, Text, Text, Text> mrd2nd;
    private MultipleInputsMapReduceDriver<Text, Text, Text, Text> mrdStageJoin;

    private IOHelper ioHelper = new IOHelper();
    private String resourcePath = String.join(File.separator, "src", "test", "resources");
    private String inputFilePath = String.join(File.separator, resourcePath, "cdr.csv");
    private String inputFilePath1 = String.join(File.separator, resourcePath, "DIM_BAN.csv");
    private String inputFilePath2 = String.join(File.separator, resourcePath, "DIM_SUBSCRIBER.csv");


    @Test
    public void allStagesTest() throws IOException {
        String firstStgFilePath = String.join(File.separator, resourcePath, "test_output1.csv");

        FirstStage.ActivityMapper mapper1st = new FirstStage.ActivityMapper();
        FirstStage.ActivityReducer reducer1st = new FirstStage.ActivityReducer();

        /*mrd1st = MultipleInputsMapReduceDriver.newMultipleInputMapReduceDriver(reducer1st).withMapper(mapper1st);

        mrd1st.addAll(mapper1st, ioHelper.read(inputFilePath));*/

        mrd1st = new MapReduceDriver<>();

        mrd1st.setMapper(mapper1st);
        mrd1st.setReducer(reducer1st);

        mrd1st.addAll(ioHelper.read(inputFilePath));
        List<Pair<Text, Text>> result1st = mrd1st.run();

        ioHelper.write(firstStgFilePath, result1st);


//-----------------------------------------------
        String secondStgFilePath = String.join(File.separator, resourcePath, "test_output2.csv");

        SecondStage.NameMapper mapper1 = new SecondStage.NameMapper();
        SecondStage.SubscriberMapper mapper2 = new SecondStage.SubscriberMapper();
        SecondStage.JoinReducer reducer = new SecondStage.JoinReducer();


        mrd2nd = MultipleInputsMapReduceDriver.newMultipleInputMapReduceDriver(reducer).withMapper(mapper1).withMapper(mapper2);

        mrd2nd.addAll(mapper1, ioHelper.read(inputFilePath1));
        mrd2nd.addAll(mapper2, ioHelper.read(inputFilePath2));
        List<Pair<Text, Text>> result = mrd2nd.run();

        ioHelper.write(secondStgFilePath, result);
//------------------------------------------------
        String joinedOutputFilePath = String.join(File.separator, resourcePath, "test_output3.csv");

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


/*mrd = new MultipleInputsMapReduceDriver<>();
        mrd.addMapper(mapper1);
        mrd.addMapper(mapper2);*/