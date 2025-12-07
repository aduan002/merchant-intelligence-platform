FROM minio/minio:RELEASE.2024-02-29T00-00-00Z

EXPOSE 9000
EXPOSE 9001

VOLUME ["/data"]

# Run with console enabled
CMD ["minio", "server", "/data", "--console-address", ":9001"]
