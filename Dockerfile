ARG TARGETPLATFORM=linux/amd64
FROM --platform=${TARGETPLATFORM} python:3.12-slim

WORKDIR /app

# Install Oracle Instant Client prerequisites
RUN apt-get update && apt-get install -y \
    libaio1t64 \
    wget \
    unzip \
    git \
    && rm -rf /var/lib/apt/lists/*

# Download and install Oracle Instant Client
RUN wget https://download.oracle.com/otn_software/linux/instantclient/2370000/instantclient-basic-linux.x64-23.7.0.25.01.zip -O /tmp/instantclient.zip \
    && unzip /tmp/instantclient.zip -d /opt \
    && rm /tmp/instantclient.zip \
    && ln -s /opt/instantclient_* /opt/instantclient_23_7 \
    # Compatibility symlink so cx_Oracle finds libaio.so.1
    && ln -sf /usr/lib/x86_64-linux-gnu/libaio.so.1t64 /usr/lib/x86_64-linux-gnu/libaio.so.1 \
    && echo /opt/instantclient_23_7 > /etc/ld.so.conf.d/oracle-instantclient.conf \
    && ldconfig

# Oracle environment variables
ENV LD_LIBRARY_PATH=/opt/instantclient_23_7
ENV PATH=/opt/instantclient_23_7:$PATH \
    TNS_ADMIN=/opt/instantclient_23_7 \
    ORACLE_DB_CLIENT=/opt/instantclient_23_7 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Runtime directories (logs + output data)
RUN mkdir -p logs data/output

# Flask API port (routes.py)
EXPOSE 5000

# Default: orquestrador (matches the sibling project pattern).
# For the API, override the command (see docker-compose.yml "api" service).
CMD ["python", "orquestrador.py"]
