FROM ontotext/graphdb:10.8.5

LABEL org.opencontainers.image.source="https://github.com/MaastrichtU-CDS/Flyover"
LABEL org.opencontainers.image.description="Reference to GraphDB for Flyover project"
LABEL org.opencontainers.image.licenses="https://graphdb.ontotext.com/documentation/10.8/licensing.html"

# Seed copied into /opt/graphdb-seed/ and applied by seed-entrypoint.sh.
# Production bind-mounts ./graphdb/data over /opt/graphdb/home, masking the seed
# and persisting real user state on the host. The seed only takes effect on
# fresh tmpfs mounts (the CI test stack). Seed files MUST stay stock GraphDB
# defaults; live user/settings state must never be copied into graphdb/seed/.
COPY seed/users.js /opt/graphdb-seed/data/users.js
COPY seed/settings.js /opt/graphdb-seed/data/settings.js
COPY data/data/repositories/userRepo/config.ttl /opt/graphdb-seed/data/repositories/userRepo/config.ttl

COPY seed-entrypoint.sh /usr/local/bin/seed-entrypoint.sh
RUN chmod +x /usr/local/bin/seed-entrypoint.sh

ENTRYPOINT ["/usr/local/bin/seed-entrypoint.sh"]
CMD ["-Dgraphdb.home=/opt/graphdb/home", "-Dgraphdb.distribution=docker"]
