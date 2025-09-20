# Multi-stage Dockerfile for Biomedical Knowledge Graph
# Stage 1: Build Python environment with conda

FROM mambaorg/micromamba:1.5.1 as python-builder

# Copy environment file
COPY kg_scripts/environment.yml /tmp/environment.yml

# Create conda environment
RUN micromamba create -f /tmp/environment.yml && \
    micromamba clean --all --yes

# Stage 2: Neo4j with APOC and GDS plugins + Python environment
FROM neo4j:5.21.2-enterprise

USER root

# Install required system packages
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    git \
    python3 \
    python3-pip \
    python3-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install miniconda
# RUN wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O /tmp/miniconda.sh && \
#     bash /tmp/miniconda.sh -b -p /opt/conda && \
#     rm /tmp/miniconda.sh
RUN wget https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Linux-$(uname -m).sh -O /tmp/miniforge.sh && \
    bash /tmp/miniforge.sh -b -p /opt/conda && \
    rm /tmp/miniforge.sh

# Add conda to PATH
ENV PATH="/opt/conda/bin:$PATH"

# Copy Python environment from builder stage
COPY --from=python-builder /opt/conda/envs/knowledge_graph /opt/conda/envs/knowledge_graph

# Activate the conda environment by default
ENV CONDA_DEFAULT_ENV=knowledge_graph
ENV PATH="/opt/conda/envs/knowledge_graph/bin:$PATH"

# Install Neo4j plugins (APOC and Graph Data Science)
ENV NEO4J_PLUGINS='["apoc", "graph-data-science"]'

# Copy knowledge graph scripts and configuration
COPY kg_scripts/ /app/kg_scripts/
COPY docker/ /app/docker/

# Create necessary directories
RUN mkdir -p /data/backups /var/lib/neo4j/import /app/logs /app/data

# Copy database dump to import directory
COPY kg_scripts/backups/biomedical-kg.dump /data/backups/

# Set proper permissions
RUN chown -R neo4j:neo4j /data /var/lib/neo4j /app && \
    chmod +x /app/docker/*.sh /app/kg_scripts/download_data.sh

# Neo4j configuration moved to docker-compose.yml

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:7474/db/manage/server/info || exit 1

# Expose ports
EXPOSE 7474 7687

# Switch back to neo4j user
USER neo4j

# Custom entrypoint that loads database on first run
ENTRYPOINT ["/app/docker/entrypoint.sh"]
CMD ["neo4j"]