# fusee server image.
FROM rust:1.95-slim AS build
WORKDIR /src
COPY . .
RUN cargo build --release --locked -p fusee-cli

FROM debian:trixie-slim
COPY --from=build /src/target/release/fusee /usr/local/bin/fusee
VOLUME /data
EXPOSE 7600
ENTRYPOINT ["fusee", "server", "--dir", "/data", "--listen", "0.0.0.0:7600"]
