package ru.digitalleague.test_mr.system;

import org.apache.hadoop.conf.Configuration;

public class ProjectParams {
    public String projectName = "";
    public String inputTestPath1 = "";
    public String inputTestPath2 = "";
    public int reducesCnt = 1;
    public String outputTestPath = "";
    public Configuration config = new Configuration();
    public int stage_number = 1;

    public enum cmdParse {
        PROJECT_NAME,
        INPUT_TEST_PATH1,
        INPUT_TEST_PATH2,
        REDUCES_CNT,
        OUTPUT_TEST_PATH,
        STAGE_NUMBER
    }

    public ProjectParams(String[] args) {
        for (int i = 0; i < args.length; i++) {
            String argSplit[] = args[i].split("=");
            if (argSplit.length == 2) {

                cmdParse currentCmd = cmdParse.valueOf(argSplit[0]);

                switch (currentCmd) {
                    case PROJECT_NAME:
                        projectName = argSplit[1];
                        break;
                    case INPUT_TEST_PATH1:
                        inputTestPath1 = argSplit[1];
                        break;
                    case INPUT_TEST_PATH2:
                        inputTestPath2 = argSplit[1];
                        break;
                    case REDUCES_CNT:
                        reducesCnt = Integer.parseInt(argSplit[1]);
                        break;
                    case OUTPUT_TEST_PATH:
                        outputTestPath = argSplit[1];
                        break;
                    case STAGE_NUMBER:
                        stage_number = Integer.parseInt(argSplit[1]);
                        break;
                    default:
                        break;
                }
            }
        }
    }
}
