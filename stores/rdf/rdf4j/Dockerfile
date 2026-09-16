FROM eclipse/rdf4j-workbench:latest

LABEL org.opencontainers.image.source="https://github.com/MaastrichtU-CDS/Flyover"
LABEL org.opencontainers.image.description="RDF4J Workbench for Flyover project"

# The base image sets a non-root user; switch to root to install files.
USER root

# Install curl and iproute2 (for ss command) needed by init-repo.sh
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    iproute2 \
    && rm -rf /var/lib/apt/lists/*

# Change Tomcat port from 8080 to 7200 so the RDF4J service is reachable on
# the same port that GraphDB uses, making it easier to switch between stores.
RUN sed -i 's/Connector port="8080"/Connector port="7200"/' \
    /usr/local/tomcat/conf/server.xml

# Replace Tomcat's default ROOT app so that visiting the bare host:port
# (http://localhost:7200/) redirects straight to the RDF4J workbench, giving
# the same "just open localhost:7200" experience as GraphDB.
RUN rm -rf /usr/local/tomcat/webapps/ROOT/*
COPY ROOT-index.html /usr/local/tomcat/webapps/ROOT/index.html

# Expose the RDF4J REST API at the server root via Tomcat's RewriteValve, so
# the triplifier connects using the same base URL (http://rdf-store:7200) as
# GraphDB and QLever — no /rdf4j-server suffix required. The valve is enabled
# at the host level; the rewrite rules live in rewrite.config.
RUN sed -i 's#</Host>#    <Valve className="org.apache.catalina.valves.rewrite.RewriteValve" />\n      </Host>#' \
    /usr/local/tomcat/conf/server.xml
RUN mkdir -p /usr/local/tomcat/conf/Catalina/localhost
COPY rewrite.config /usr/local/tomcat/conf/Catalina/localhost/rewrite.config

# Copy the repository configuration and the init script.
COPY config.ttl /opt/rdf4j-config/config.ttl
COPY init-repo.sh /usr/local/bin/init-repo.sh
RUN chmod +x /usr/local/bin/init-repo.sh

EXPOSE 7200

ENTRYPOINT ["/usr/local/bin/init-repo.sh"]
