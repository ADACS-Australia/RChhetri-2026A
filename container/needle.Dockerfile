ARG BASE_VERSION="latest"
FROM needle-base:${BASE_VERSION}

WORKDIR /opt/needle
COPY pyproject.toml .
COPY uv.lock .

# Copy dependency files first — separate layer so it's only reinstalled when dependencies change
COPY pyproject.toml .
COPY uv.lock .
RUN uv pip install --system --prefix=/opt/needle -r pyproject.toml && \
    # Drop this becuase we install it with other packages and it causes conflicts
    rm /opt/needle/lib/python3.12/site-packages/casatools/__casac__/lib/libsqlite3.so.0 

COPY README.md .
COPY needle/ ./needle/
RUN uv pip install --system --prefix=/opt/needle --no-deps .

ENV PATH="/opt/needle/bin:${PATH}"
ENV PYTHONPATH="/opt/needle/lib/python3.12/site-packages:${PYTHONPATH}"
ENV CASARC="/opt/needle/.casa/config.py"
ENV HOME="/opt/needle"

COPY container/casa_config.py /opt/needle/.casa/config.py

# Fix permissions on the data mount point at build time
RUN mkdir -p /data && \
    chmod 777 /data && \
    mkdir -p /opt/needle/.casa/data && \
    chown -R root:root /opt/needle/.casa/data && \
    chmod 777 /opt/needle/.casa/data 


