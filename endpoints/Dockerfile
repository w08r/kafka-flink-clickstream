FROM rust:1.81.0-slim@sha256:3e24ad2190a3a1b91532478cd48b4e36fdcfc0eb4ee89a34e064c145febe360c AS builder

RUN apt update -y
RUN apt install -y pkg-config libssl-dev libfindbin-libs-perl build-essential cmake || \
    apt install -y pkg-config libssl-dev libfindbin-libs-perl build-essential cmake

WORKDIR /work
COPY . .
RUN cargo install --path .

FROM debian:bookworm-slim@sha256:a629e796d77a7b2ff82186ed15d01a493801c020eed5ce6adaa2704356f15a1c
COPY --from=builder /usr/local/cargo/bin/ok /usr/local/bin/ok
COPY --from=builder /usr/local/cargo/bin/producer /usr/local/bin/producer
COPY --from=builder /usr/local/cargo/bin/consumer /usr/local/bin/consumer

