FROM adfreiburg/qlever:latest

LABEL org.opencontainers.image.source="https://github.com/MaastrichtU-CDS/Flyover"
LABEL org.opencontainers.image.description="QLever engine for Flyover project"

# Seed used to build an (initially empty) index on first start. The triplifier
# subsequently loads the real data through SPARQL UPDATE via the adapter.
COPY seed.nt /seed/seed.nt
# The QLever base image runs as a non-root user, so a `RUN chmod` on a
# root-owned file fails with "Operation not permitted". Set the executable
# bit at copy time instead, which needs no extra privileges.
COPY --chmod=0755 entrypoint.sh /usr/local/bin/qlever-entrypoint.sh

EXPOSE 7019

ENTRYPOINT ["/usr/local/bin/qlever-entrypoint.sh"]
