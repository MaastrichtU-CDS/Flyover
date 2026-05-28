FROM ontotext/graphdb:10.8.5

LABEL org.opencontainers.image.source="https://github.com/MaastrichtU-CDS/Flyover"
LABEL org.opencontainers.image.description="Reference to GraphDB for Flyover project"
LABEL org.opencontainers.image.licenses="https://graphdb.ontotext.com/documentation/10.8/licensing.html"

COPY data/data/users.js /opt/graphdb-seed/data/users.js
COPY data/data/settings.js /opt/graphdb-seed/data/settings.js
COPY data/data/repositories/userRepo/config.ttl /opt/graphdb-seed/data/repositories/userRepo/config.ttl

COPY seed-entrypoint.sh /usr/local/bin/seed-entrypoint.sh
RUN chmod +x /usr/local/bin/seed-entrypoint.sh

ENTRYPOINT ["/usr/local/bin/seed-entrypoint.sh"]
CMD ["-Dgraphdb.home=/opt/graphdb/home", "-Dgraphdb.distribution=docker"]
