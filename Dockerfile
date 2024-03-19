FROM --platform=$BUILDPLATFORM rust:1.76 as builder
RUN apt-get update && apt-get install -y \
    g++-x86-64-linux-gnu libc6-dev-amd64-cross \
    g++-aarch64-linux-gnu libc6-dev-arm64-cross \
    g++-arm-linux-gnueabihf libc6-dev-armhf-cross && \
    rm -rf /var/lib/apt/lists/*
RUN rustup target add \
    x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu armv7-unknown-linux-gnueabihf
RUN rustup toolchain install \
    1.76-x86_64-unknown-linux-gnu 1.76-aarch64-unknown-linux-gnu 1.76-armv7-unknown-linux-gnueabihf
RUN rustup component add rustfmt
ENV CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=x86_64-linux-gnu-gcc \
    CC_x86_64_unknown_linux_gnu=x86_64-linux-gnu-gcc \
    CXX_x86_64_unknown_linux_gnu=x86_64-linux-gnu-g++ \
    CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc \
    CC_aarch64_unknown_linux_gnu=aarch64-linux-gnu-gcc \
    CXX_aarch64_unknown_linux_gnu=aarch64-linux-gnu-g++ \
    CARGO_TARGET_ARMV7_UNKNOWN_LINUX_GNUEABIHF_LINKER=arm-linux-gnueabihf-gcc \
    CC_armv7_unknown_linux_gnueabihf=arm-linux-gnueabihf-gcc \
    CXX_armv7_unknown_linux_gnueabihf=arm-linux-gnueabihf-g++ \
    CARGO_INCREMENTAL=0

FROM --platform=$BUILDPLATFORM builder AS base-dir
WORKDIR /app
COPY src src
COPY bin bin
COPY Cargo.toml Cargo.toml

# amd64 build ----------------------------
FROM --platform=$BUILDPLATFORM base-dir AS build-amd64
RUN cargo build --profile release -p trident-storage --target x86_64-unknown-linux-gnu
RUN mv target/x86_64-unknown-linux-gnu/release/trident-storage /trident-storage

# arm64 build ----------------------------
FROM --platform=$BUILDPLATFORM base-dir AS build-arm64
RUN CC=aarch64-linux-gnu-gcc CXX=aarch64-linux-gnu-g++ cargo build --profile release -p trident-storage --target aarch64-unknown-linux-gnu
RUN mv target/aarch64-unknown-linux-gnu/release/trident-storage /trident-storage

# armv7 build ----------------------------
FROM --platform=$BUILDPLATFORM base-dir AS build-arm
RUN CC=arm-linux-gnueabihf-gcc CXX=arm-linux-gnueabihf-g++ cargo build --profile release -p trident-storage --target armv7-unknown-linux-gnueabihf
RUN mv target/armv7-unknown-linux-gnueabihf/release/trident-storage /trident-storage

# Final arch images ----------------------
FROM --platform=amd64 gcr.io/distroless/cc-debian12 AS final-amd64
COPY --from=build-amd64 /trident-storage /trident-storage

FROM --platform=arm64 gcr.io/distroless/cc-debian12:latest-arm64 AS final-arm64
COPY --from=build-arm64 /trident-storage /trident-storage

FROM --platform=arm/v7 gcr.io/distroless/cc-debian12:latest-arm AS final-arm
COPY --from=build-arm /trident-storage /trident-storage

# Final image ----------------------------
FROM final-${TARGETARCH}
ENTRYPOINT ["./trident-storage"]