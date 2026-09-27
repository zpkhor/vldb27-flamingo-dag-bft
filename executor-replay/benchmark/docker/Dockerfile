FROM ubuntu:24.04

RUN apt-get update && apt-get install -y --no-install-recommends \
    iproute2 \
    && rm -rf /var/lib/apt/lists/*

COPY entrypoint.sh /entrypoint.sh
COPY client-entrypoint.sh /client-entrypoint.sh
COPY executor-entrypoint.sh /executor-entrypoint.sh
RUN chmod +x /entrypoint.sh /client-entrypoint.sh /executor-entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
