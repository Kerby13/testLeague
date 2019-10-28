package ru.digitalleague.test_mr.system;

import org.apache.hadoop.conf.Configuration;

public class ProjectParams {
    public String projectName = "";
    public String inputTestPath = "";
    public int reducesCnt = 1;
    public String outputTestPath = "";
    public Configuration config = new Configuration();

public enum cmdParse {
    PROJECT_NAME,
    INPUT_TEST_PATH,
    REDUCES_CNT,
    OUTPUT_TEST_PATH,


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
                    case INPUT_TEST_PATH:
                        inputTestPath = argSplit[1];
                        break;
                    case REDUCES_CNT:
                        reducesCnt = Integer.valueOf(argSplit[1]);
                        break;
                    case OUTPUT_TEST_PATH:
                        outputTestPath = argSplit[1];
                        break;
                    default:
                        break;
                }
            }
        }
    }
}
