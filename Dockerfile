# Runtime base (glibc for Deno-compiled binary)
FROM debian:bookworm-slim

WORKDIR /app

# Copy compiled binary
ARG BIN_NAME=scanner-linux-x64
COPY ${BIN_NAME} /app/scanner

# Ensure executable
RUN chmod +x /app/scanner \
  && useradd -u 10001 -m -d /home/scanner -s /usr/sbin/nologin scanner \
  && mkdir -p /app/logs \
  && chown -R scanner:scanner /app

ENV PORT=8000

# Expose HTTP
EXPOSE 8000

# Run scanner from logs directory so relative JSON paths land in /app/logs
WORKDIR /app/logs
CMD ["sh", "-c", "/app/scanner --port ${PORT}"]
