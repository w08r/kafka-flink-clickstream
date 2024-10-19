FROM openjdk:22-slim-bullseye as builder

COPY . /cp

WORKDIR /cp

RUN ./mvnw package

FROM flink:latest
RUN mkdir /cp
COPY --from=builder /cp/target/f-1.0-SNAPSHOT.jar /cp