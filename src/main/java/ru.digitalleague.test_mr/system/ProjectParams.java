package ru.digitalleague.test_mr.system;

import org.apache.hadoop.conf.Configuration;

public class ProjectParams {
    public String projectName = "";
    public String cmdInputPath = "";
    public String dimBanInputPath = "";
    public String subscriberInputPath = "";
    public int reducesCnt = 1;
    public String stage1_outputPath = "";
    public String stage2_outputPath = "";
    public String stage3_outputPath = "";
    public Configuration config = new Configuration();
    public int stage_number = 1;
    public String queue_name = "";

    public enum cmdParse {
        PROJECT_NAME,
        CMD_INPUT_PATH,
        DIM_BAN_INPUT_PATH,
        DIM_SUBSCRIBER_INPUT_PATH,
        REDUCES_CNT,
        STAGE_1_OUTPUT_PATH,
        STAGE_2_OUTPUT_PATH,
        STAGE_3_OUTPUT_PATH,
        STAGE_NUMBER,
        QUEUE_NAME
    }

    public ProjectParams(String[] args) {
        for (int i = 0; i < args.length; i++) {
            String argSplit[] = args[i].split("=", 2);
            //if (argSplit.length == 2) {

            cmdParse currentCmd = cmdParse.valueOf(argSplit[0]);

            switch (currentCmd) {
                case PROJECT_NAME:
                    projectName = argSplit[1];
                    break;
                case CMD_INPUT_PATH:
                    cmdInputPath = argSplit[1];
                    break;
                case DIM_BAN_INPUT_PATH:
                    dimBanInputPath = argSplit[1];
                    break;
                case DIM_SUBSCRIBER_INPUT_PATH:
                    subscriberInputPath = argSplit[1];
                    break;
                case REDUCES_CNT:
                    reducesCnt = Integer.parseInt(argSplit[1]);
                    break;
                case STAGE_1_OUTPUT_PATH:
                    stage1_outputPath = argSplit[1];
                    break;
                case STAGE_2_OUTPUT_PATH:
                    stage2_outputPath = argSplit[1];
                    break;
                case STAGE_3_OUTPUT_PATH:
                    stage3_outputPath = argSplit[1];
                    break;
                case STAGE_NUMBER:
                    stage_number = Integer.parseInt(argSplit[1]);
                    break;
                case QUEUE_NAME:
                    queue_name = argSplit[1];
                    break;
                default:
                    break;
            }
            // }
        }
    }
}
