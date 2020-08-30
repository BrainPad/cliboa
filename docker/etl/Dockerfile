FROM debian:stable-slim

# Install essential packages
RUN apt update -y && \
    apt install -y git \
                   libc6-dev \
                   libbz2-dev \
                   libffi-dev \
                   libgdbm-dev \
                   libssl-dev \
                   libsqlite3-dev \
                   openssl \
                   tk-dev \
                   python \
                   python-pip \
                   python3 \
                   python3-pip \
                   vim \
                   virtualenv \
                   wget \
                   zlib1g-dev

# Download multiple python versions
RUN cd /usr/local/ && \
    wget https://www.python.org/ftp/python/3.5.9/Python-3.5.9.tgz  \
    tar -xzvf Python-3.5.9.tgz \
    wget https://www.python.org/ftp/python/3.6.9/Python-3.6.9.tgz \
    tar -xzvf Python-3.6.9.tgz \
    wget https://www.python.org/ftp/python/3.8.5/Python-3.8.5.tgz \
    tar -xzvf Python-3.8.5.tgz

# Build multiple python versions
RUN cd /usr/local/Python-3.5.9 && \
    ./configure --prefix=/usr/local/bin/python3.5 && \
    make && \
    make altinstall
RUN cd /usr/local/Python-3.6.9 && \
    ./configure --prefix=/usr/local/bin/python3.6 && \
    make && \
    make altinstall

