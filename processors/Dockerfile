FROM clojure:tools-deps-bullseye-slim AS builder

WORKDIR /cp

COPY deps.edn .
RUN clj -X:deps prep

COPY . .
RUN clj -T:build uber

FROM flink:1.20.0-scala_2.12-java17
RUN mkdir /cp
COPY --from=builder /cp/target/processor-0.0.0-standalone.jar /cp