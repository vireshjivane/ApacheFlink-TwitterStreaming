# ApacheFlink-TwitterStreaming

docker pull flink

docker network create overlay

docker run --name flink_jobmanager --env JOB_MANAGER_RPC_ADDRESS={HOST_IP} -p 8081:8081 -p 6123:6123 -p 48081:48081 -p 6124:6124 -p 6125:6125 --network overlay -d -t flink jobmanager

docker run --name flink_taskmanager --env JOB_MANAGER_RPC_ADDRESS={HOST_IP} -p 6121:6121 -p 6122:6122 --network overlay -d -t flink taskmanager

JOBMANAGER_CONTAINER=$(docker ps --filter name=jobmanager --format={{.ID}})

docker cp ~/apache-flink-twitter-streaming.jar "$JOBMANAGER_CONTAINER":/job.jar

docker exec -t -i "$JOBMANAGER_CONTAINER" flink run /job.jar --twitter-source.consumerKey xxxxx --twitter-source.consumerSecret xxxxx --twitter-source.token xxxxx --twitter-source.tokenSecret xxxxx

docker kill flink_jobmanager && docker rm flink_jobmanager

docker kill flink_taskmanager && docker rm flink_taskmanager

docker network rm overlay
