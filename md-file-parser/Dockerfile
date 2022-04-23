FROM openjdk:11
ARG APP_NAME="md-file-parser"
ARG APP_VERSION="0.0.1"
ARG JAR_FILE="${APP_NAME}-${APP_VERSION}-SNAPSHOT.jar"
ADD target/${JAR_FILE} app.jar
EXPOSE 8185
ENTRYPOINT ["java","-jar","-Dspring.profiles.active=docker","/app.jar"]