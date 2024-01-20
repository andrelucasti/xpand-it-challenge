FROM maven:3-amazoncorretto-11 as builder
COPY ./ ./
RUN mvn clean package

FROM amazoncorretto:11
WORKDIR /usr/src/app
COPY --from=builder /target/*.jar /usr/src/app/

ENTRYPOINT java -jar "app.jar"
#CMD sleep infinity