projectName="testMavenMapReduce" 
jarName=mapReduce-1.0-SNAPSHOT.jar  
mainClass="ru.digitalleague.test_mr.system.TestMain" 

reducers=1 

stage1InputPath="/user/cmd/storage/MAF5/20171007/*" 

stage2InputPath1="/apps/hive/warehouse/biis.db/dim_ban/market_key=PBUR" 

stage2InputPath2="/apps/hive/warehouse/biis.db/dim_subscriber/market_key=PBUR" 

stage3InputPath1="/user/pshevlyakova/nalukin/output/stage1" 

stage3InputPath2="/user/pshevlyakova/nalukin/output/stage2" 

outputPath="/user/pshevlyakova/nalukin/output"


hadoop fs -rm -R ${outputPath} 

hadoop jar ${jarName} ${mainClass} \
PROJECT_NAME=${projectName} \
STAGE1_INPUT_PATH=${stage1InputPath} \
STAGE2_INPUT_PATH=${stage1InputPath} \
OUTPUT_PATH=${stage3InputPath1} \
REDUCES_CNT=${reducers} \
STAGE_NUMBER=1 \
QUEUE_NAME=adhoc

#hadoop jar ${jarName} ${mainClass} \
#PROJECT_NAME=${projectName} \
#STAGE1_INPUT_PATH=${stage2InputPath1} \
#STAGE2_INPUT_PATH=${stage2InputPath2} \
#OUTPUT_PATH=${stage3InputPath2} \
#REDUCES_CNT=${reducers} \
#STAGE_NUMBER=2 \
#QUEUE_NAME=adhoc


#hadoop jar ${jarName} ${mainClass} \
#PROJECT_NAME=${projectName} \
#STAGE1_INPUT_PATH=${stage3InputPath1} \
#STAGE2_INPUT_PATH=${stage3InputPath2} \
#OUTPUT_PATH=${outputPath} \
#REDUCES_CNT=${reducers} \
#STAGE_NUMBER=3 \
#QUEUE_NAME=adhoc

