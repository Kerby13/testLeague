/*
package ru.digitalleague.test_mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.MultipleInputsMapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Test;
import ru.digitalleague.test_mr.staging.Stage1.ActivityMapper;
import ru.digitalleague.test_mr.staging.Stage1.FirstStageReducer;
import ru.digitalleague.test_mr.staging.Stage2.NameMapper;
import ru.digitalleague.test_mr.staging.Stage2.PhoneMapper;
import ru.digitalleague.test_mr.staging.Stage2.SecondStageReducer;
import ru.digitalleague.test_mr.staging.Stage3.FirstResultMapper;
import ru.digitalleague.test_mr.staging.Stage3.SecondResultMapper;
import ru.digitalleague.test_mr.staging.Stage3.ThirdStageReducer;
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

        ActivityMapper mapper1st = new ActivityMapper();
        FirstStageReducer reducer1st = new FirstStageReducer();

        */
/*mrd1st = MultipleInputsMapReduceDriver.newMultipleInputMapReduceDriver(reducer1st).withMapper(mapper1st);

        mrd1st.addAll(mapper1st, ioHelper.read(inputFilePath));*//*


        mrd1st = new MapReduceDriver<>();

        mrd1st.setMapper(mapper1st);
        mrd1st.setReducer(reducer1st);

        mrd1st.addAll(ioHelper.read(inputFilePath));
        List<Pair<Text, Text>> result1st = mrd1st.run();

        ioHelper.write(firstStgFilePath, result1st);


//-----------------------------------------------
        String secondStgFilePath = String.join(File.separator, resourcePath, "test_output2.csv");

        NameMapper mapper1 = new NameMapper();
        PhoneMapper mapper2 = new PhoneMapper();
        SecondStageReducer reducer = new SecondStageReducer();


        mrd2nd = MultipleInputsMapReduceDriver.newMultipleInputMapReduceDriver(reducer).withMapper(mapper1).withMapper(mapper2);

        mrd2nd.addAll(mapper1, ioHelper.read(inputFilePath1));
        mrd2nd.addAll(mapper2, ioHelper.read(inputFilePath2));
        List<Pair<Text, Text>> result = mrd2nd.run();

        ioHelper.write(secondStgFilePath, result);
//------------------------------------------------
        String joinedOutputFilePath = String.join(File.separator, resourcePath, "test_output3.csv");

        FirstResultMapper joinMapper1 = new FirstResultMapper();
        SecondResultMapper joinMapper2 = new SecondResultMapper();
        ThirdStageReducer joinReducer = new ThirdStageReducer();

        mrdStageJoin = MultipleInputsMapReduceDriver.newMultipleInputMapReduceDriver(joinReducer).withMapper(joinMapper1).withMapper(joinMapper2);

        mrdStageJoin.addAll(joinMapper1, ioHelper.read(firstStgFilePath));
        mrdStageJoin.addAll(joinMapper2, ioHelper.read(secondStgFilePath));
        List<Pair<Text, Text>> joinedResult = mrdStageJoin.run();

        ioHelper.write(joinedOutputFilePath, joinedResult);

    }
}


*/
/*mrd = new MultipleInputsMapReduceDriver<>();
        mrd.addMapper(mapper1);
        mrd.addMapper(mapper2);*/
