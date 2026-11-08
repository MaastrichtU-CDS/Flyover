FROM python:3.12-alpine

LABEL org.opencontainers.image.source="https://github.com/MaastrichtU-CDS/Flyover"
LABEL org.opencontainers.image.description="Starts the store containers Flyover needs, when it needs them (Flyover project)"

# Standard library only: nothing to install. The launcher runs as root to
# use Docker's socket, and does no more with it than start and inspect the
# stack's labelled store containers (see launcher.py).
COPY launcher.py /app/launcher.py

EXPOSE 7290

CMD ["python", "-u", "/app/launcher.py"]
