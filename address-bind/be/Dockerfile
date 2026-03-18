FROM m.daocloud.io/docker.io/rust:slim-bullseye AS buildstage
WORKDIR /build
ENV PROTOC_NO_VENDOR 1
RUN rustup component add rustfmt && \
    apt-get update && \
    apt-get install -y --no-install-recommends pkg-config libssl-dev  && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
COPY . /build/
RUN cargo build --release

FROM m.daocloud.io/docker.io/rust:slim-bullseye
RUN useradd -m address-bind-be
USER address-bind-be
COPY --from=buildstage /build/target/release/address-bind-be /usr/bin/
CMD ["address-bind-be", "indexer", "--db-url $DB_URL"]
