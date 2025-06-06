FROM python:3.12-slim-bookworm

RUN rm -f /etc/apt/apt.conf.d/docker-clean; \
    echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' > /etc/apt/apt.conf.d/keep-cache

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update -y && apt-get upgrade -y && apt-get install -y git g++

WORKDIR /billing-collector

ADD LICENSE requirements.txt ./
ADD billing_collector ./billing_collector/
ADD pyproject.toml ./

RUN --mount=type=cache,target=/root/.cache/pip pip3 install -r requirements.txt .

CMD ["python", "-m", "billing_collector"]
