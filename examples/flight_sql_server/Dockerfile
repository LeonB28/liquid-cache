FROM ubuntu:22.04

RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

ENV RUST_BACKTRACE=1
ENV RUST_LOG=info
ENV PORT1=15215
ENV PORT2=15216

WORKDIR /app

COPY /target/release/flight_sql_server /app/flight_sql_server

EXPOSE 15215 15216