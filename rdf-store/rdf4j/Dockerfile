FROM eclipse/rdf4j-workbench:latest

LABEL org.opencontainers.image.source="https://github.com/MaastrichtU-CDS/Flyover"
LABEL org.opencontainers.image.description="RDF4J Workbench for Flyover project"

# The base image sets a non-root user; switch to root to install files.
USER root

# Change Tomcat port from 8080 to 7200 so the RDF4J service is reachable on
# the same port that GraphDB uses, making it easier to switch between stores.
RUN sed -i 's/Connector port="8080"/Connector port="7200"/' \
    /usr/local/tomcat/conf/server.xml

# Copy the repository configuration and the init script.
COPY config.ttl /opt/rdf4j-config/config.ttl
COPY init-repo.sh /usr/local/bin/init-repo.sh
RUN chmod +x /usr/local/bin/init-repo.sh

EXPOSE 7200

ENTRYPOINT ["/usr/local/bin/init-repo.sh"]
