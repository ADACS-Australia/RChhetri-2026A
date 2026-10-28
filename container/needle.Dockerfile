ARG BASE_VERSION="latest"
ARG BASE_IMAGE="needle-base"
FROM ${BASE_IMAGE}:${BASE_VERSION}

WORKDIR /opt/needle
COPY pyproject.toml .
COPY uv.lock .

# Copy dependency files first — separate layer so it's only reinstalled when dependencies change
COPY pyproject.toml .
COPY uv.lock .
RUN uv pip install --system --prefix=/opt/needle -r pyproject.toml

# Remove CASA's bundled sqlite3 if it was installed into the project prefix.
# It shadows the system library and breaks Python's sqlite3 extension.
RUN if [ -d /opt/needle/lib/python3.12/site-packages/casatools ]; then \
        find /opt/needle/lib/python3.12/site-packages/casatools -name "libsqlite3.so*" -print -delete; \
    fi

COPY README.md .
COPY needle/ ./needle/
RUN uv pip install --system --prefix=/opt/needle --no-deps .

ENV PATH="/opt/needle/bin:${PATH}"
ENV PYTHONPATH="/opt/needle/lib/python3.12/site-packages:${PYTHONPATH}"
# ENV CASARC="/opt/needle/.casa/config.py"
ENV HOME="/opt/needle"


# Fix permissions on the data mount point at build time
RUN mkdir -p /data && \
    chmod 777 /data 
    # mkdir -p /opt/needle/.casa/data && \
    # chown -R root:root /opt/needle/.casa/data && \
    # chmod 777 /opt/needle/.casa/data 


