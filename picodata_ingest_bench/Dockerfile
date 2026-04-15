FROM almalinux:8

ARG RUST_VERSION=1.89

ENV PATH=/root/.cargo/bin:/root/.local/bin:${PATH}
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true

RUN dnf -y install dnf-plugins-core epel-release \
    && dnf config-manager --set-enabled powertools \
    && dnf module -y enable nodejs:20 \
    && dnf install -y --nobest \
        bzip2-devel \
        cmake \
        cyrus-sasl-devel \
        curl \
        curl-devel \
        findutils \
        gcc \
        gcc-c++ \
        git \
        glibc-langpack-en \
        libffi-devel \
        libicu-devel \
        libstdc++-static \
        libtool \
        libunwind \
        libunwind-devel \
        libyaml-devel \
        libzstd-devel \
        make \
        nodejs \
        openldap-devel \
        openssl-devel \
        patch \
        perf \
        perl-interpreter \
        perl-open \
        pkgconfig \
        procps-ng \
        python3.11 \
        python3.11-pip \
        readline-devel \
        which \
        zlib-devel \
    && dnf clean all \
    && ln -sf /usr/bin/python3.11 /usr/bin/python3 \
    && ln -sf /usr/bin/python3.11 /usr/bin/python

RUN set -e; \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | \
    sh -s -- -y --profile default --default-toolchain ${RUST_VERSION}

RUN python3.11 -m pip install --no-cache-dir poetry

WORKDIR /work/picodata
