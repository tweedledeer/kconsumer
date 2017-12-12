FROM java:8
MAINTAINER me
WORKDIR /Users/gkarabut/src/kafkaconsumer
ADD . /Users/gkarabut/src/kafkaconsumer
ENV NAME World
RUN touch output.log
CMD java -jar build/libs/kafkaconsumer-1.0-SNAPSHOT-all.jar
