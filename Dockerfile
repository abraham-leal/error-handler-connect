FROM maven:3.6.0-jdk-8-slim AS build
COPY src /home/app/src
COPY pom.xml /home/app
RUN mvn -f /home/app/pom.xml clean compile io.confluent:kafka-connect-maven-plugin:kafka-connect

FROM confluentinc/cp-kafka-connect:6.1.1

COPY --from=build /home/app/target/components/packages/abrahamleal-error-handler-connect-1.0-SNAPSHOT /usr/share/confluent-hub-components/


