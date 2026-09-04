# Pipeline image: Hail + bcftools + tabix + JDK 17 on Debian.
# Build with: scripts/setup_env.sh docker
# Pipeline code and data are bind-mounted at run time, not baked into the image.

FROM python:3.11-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        bcftools \
        tabix \
        libopenblas0 \
        liblapack3 \
    && rm -rf /var/lib/apt/lists/*

# Hail embeds the JVM via JPype, which resolves it from JAVA_HOME; resolve the
# real JVM directory so the image works on both amd64 and arm64.
RUN ln -sfn \
        "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" \
        /usr/lib/jvm/current
ENV JAVA_HOME=/usr/lib/jvm/current

RUN pip install --no-cache-dir hail==0.2.135

WORKDIR /work
CMD ["bash"]
