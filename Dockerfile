FROM confluentinc/cp-kafka-connect:6.1.1

USER root

COPY . /home/appuser
ENV JAVA_HOME="/usr/lib/jvm/zulu-11"
RUN yum install -y maven
RUN mvn clean compile io.confluent:kafka-connect-maven-plugin:kafka-connect -PdockerBuild
COPY ./build/components/packages/error-handler-connect-1.0-SNAPSHOT /usr/share/confluent-hub-components/error-handler/

USER appuser


