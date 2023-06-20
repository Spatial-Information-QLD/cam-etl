FROM mcr.microsoft.com/devcontainers/python:3.10 as builder

WORKDIR /app

RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends \
        bash \
        curl \
        gdal-bin \
        libclang-dev

RUN curl https://sh.rustup.rs -sSf | bash -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

RUN python -m venv /opt/venv
ENV VIRTUALENV=/opt/venv \
    PATH=/opt/venv/bin:${PATH}

COPY requirements.txt .

RUN pip install -r requirements.txt
RUN pip install gunicorn

COPY cam/ cam/

EXPOSE 8000

CMD [ "gunicorn", "-w", "1", "--bind=:8000", "--forwarded-allow-ips='*'", "cam.web.app:app" ]
