FROM eclipse/rdf4j-workbench:latest

LABEL org.opencontainers.image.source="https://github.com/MaastrichtU-CDS/Flyover"
LABEL org.opencontainers.image.description="RDF4J Workbench for Flyover project"

# The base image sets a non-root user; switch to root to install files.
USER root

# Copy the repository configuration and the init script.
COPY config.ttl /opt/rdf4j-config/config.ttl
COPY init-repo.sh /usr/local/bin/init-repo.sh
RUN chmod +x /usr/local/bin/init-repo.sh

ENTRYPOINT ["/usr/local/bin/init-repo.sh"]
