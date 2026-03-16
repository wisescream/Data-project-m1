FROM debian:bookworm-slim

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends rsyslog ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /work /var/log/logstorm

CMD ["rsyslogd", "-n", "-f", "/etc/rsyslog.conf"]
