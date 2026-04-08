#!/bin/bash
set -e

# Start Tomcat in the background so we can run initialisation tasks.
catalina.sh start

echo "Waiting for RDF4J server to be ready..."
for i in $(seq 1 60); do
    if curl -sf http://localhost:8080/rdf4j-server/protocol > /dev/null 2>&1; then
        echo "RDF4J server is ready."
        break
    fi
    sleep 1
done

# Create the repository if it does not already exist.
if curl -sf http://localhost:8080/rdf4j-server/repositories/userRepo/size > /dev/null 2>&1; then
    echo "Repository 'userRepo' already exists — skipping creation."
else
    echo "Creating repository 'userRepo'..."
    curl -X PUT \
        -H "Content-Type: text/turtle" \
        -d @/opt/rdf4j-config/config.ttl \
        "http://localhost:8080/rdf4j-server/repositories/userRepo"
    echo ""
    echo "Repository 'userRepo' created successfully."
fi

# Stop the background Tomcat instance gracefully.
catalina.sh stop 2>/dev/null || true
sleep 2

# Restart Tomcat in the foreground as PID 1.
exec catalina.sh run
