projectName="testMavenMapReduce" 
jarName=mapReduce-1.0-SNAPSHOT.jar  
mainClass="ru.digitalleague.test_mr.system.TestMain" 

reducers=1 

firstInputPath="/user/cmd/storage/MAF5/20171007/*" 

second1InputPath="/apps/hive/warehouse/biis.db/dim_ban/market_key=PBUR" 

second2InputPath="/apps/hive/warehouse/biis.db/dim_subscriber/market_key=PBUR" 

third1InputPath="/user/pshevlyakova/nalukin/output" 

third2InputPath="/user/pshevlyakova/nalukin/output" 

testOutputPath="/user/pshevlyakova/nalukin/output"


hadoop fs -rm -R ${testOutputPath} 

hadoop jar ${jarName} ${mainClass} \
PROJECT_NAME=${projectName} \
INPUT_TEST_PATH1=${firstInputPath} \
INPUT_TEST_PATH2=${firstInputPath} \
OUTPUT_TEST_PATH=${testOutputPath} \
REDUCES_CNT=${reducers} \
STAGE_NUMBER=${1} \
QUEUE_NAME=adhoc

hadoop jar ${jarName} ${mainClass} \
PROJECT_NAME=${projectName} \
INPUT_TEST_PATH1=${second1InputPath} \
INPUT_TEST_PATH2=${second2InputPath} \
OUTPUT_TEST_PATH=${testOutputPath} \
REDUCES_CNT=${reducers} \
STAGE_NUMBER=${2} \
QUEUE_NAME=adhoc


hadoop jar ${jarName} ${mainClass} \
PROJECT_NAME=${projectName} \
INPUT_TEST_PATH1=${third1InputPath} \
INPUT_TEST_PATH2=${third2InputPath} \
OUTPUT_TEST_PATH=${testOutputPath} \
REDUCES_CNT=${reducers} \
STAGE_NUMBER=${3} \
QUEUE_NAME=adhoc

