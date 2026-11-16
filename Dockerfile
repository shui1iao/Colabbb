FROM python:3.12-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHON_KEYRING_BACKEND=keyrings.alt.file.PlaintextKeyring \
    COLABBB_HOME=/app \
    COLABBB_STATE_DIR=/data/state \
    COLABBB_OUTPUT_DIR=/data/outputs \
    HOME=/data/home \
    COLABBB_PANEL_HOST=0.0.0.0 \
    COLABBB_PANEL_PORT=17860 \
    COLABBB_TUNNEL_PORT=22222 \
    COLABBB_TUNNEL_USER=tunnel \
    COLABBB_COMFY_PROXY_HOST=0.0.0.0

RUN apt-get update \
    && apt-get install -y --no-install-recommends openssh-client openssh-server ffmpeg tini ca-certificates tzdata \
    && rm -rf /var/lib/apt/lists/* /etc/ssh/ssh_host_* \
    && useradd --system --create-home --home-dir /home/tunnel --shell /usr/sbin/nologin --password '*' tunnel
RUN python -m pip install google-colab-cli==0.8.0 keyrings.alt==5.0.2 Pillow==13.0.0
WORKDIR /app
COPY . /app
RUN chmod 755 /app/docker/entrypoint.sh /app/colab-ai \
    && ln -s /app/colab-ai /usr/local/bin/colab-ai \
    && mv /usr/local/bin/colab /usr/local/bin/colab-upstream \
    && install -m 755 /app/docker/colab.sh /usr/local/bin/colab
EXPOSE 17860 2222 18188
VOLUME ["/data"]
ENTRYPOINT ["/usr/bin/tini", "-g", "--", "/app/docker/entrypoint.sh"]
CMD ["python3", "/app/web_panel.py"]
