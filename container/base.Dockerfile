FROM python:3.12-slim

ENV WSCLEAN_VERSION="3.6"
ENV CASA_VERSION="6.7.3.21"
ENV AEGEAN_VERSION="2.3.5"

# Ignore annoying casa syntax warnings
ENV PYTHONWARNINGS=ignore::SyntaxWarning

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    wget \
    cmake \
    gfortran \
    g++ \
    git \
    libncurses5-dev \
    libreadline-dev \
    flex \
    bison \
    libblas-dev \
    liblapack-dev \
    libcfitsio-dev \
    wcslib-dev \
    libsigc++-2.0-dev \
    libgsl-dev \
    libfftw3-dev \
    libhdf5-dev \
    libxml2 \
    libxml2-dev \
    libpng-dev \
    libboost-all-dev \
    libsqlite3-dev \   
    casacore-dev \
    casacore-tools \
    lua5.3 \
    liblua5.3-dev && \
    pip install pybind11 && \
    rm -rf /var/lib/apt/lists/*

# WSclean build 
RUN wget -O wsclean-v${WSCLEAN_VERSION}.tar.bz2 https://gitlab.com/aroffringa/wsclean/-/package_files/173938999/download && \ 
    bzcat wsclean-v${WSCLEAN_VERSION}.tar.bz2 | tar -xvf - && \
    cd wsclean-v${WSCLEAN_VERSION} && \
    mkdir -p build && cd build && cmake .. && make -j 4 && make install && \
    cd .. && rm -rf wsclean-v*

# Install UV because it's faster than pip installing
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
RUN uv pip install --system "casatasks==${CASA_VERSION}" "casatools==${CASA_VERSION}" "aegeantools==${AEGEAN_VERSION}"



