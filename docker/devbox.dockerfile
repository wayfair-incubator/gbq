FROM python:3.14-bookworm@sha256:7ee7e4d4fb42c3ad45b8fdc473b64ec69c3cf6e80ae5d52f6fa55f77fac29027

# Pinned uv binary, copied from the official image instead of piping an install script to sh
COPY --from=ghcr.io/astral-sh/uv:0.12.21@sha256:a7aed3216253ee804de3e2d8afa5073baa1a177335345d43845cd4165e43b711 /uv /uvx /usr/local/bin/

ARG _USER="gbq"
ARG _UID="1000"
ARG _GID="100"
ARG _SHELL="/bin/bash"

RUN useradd -m -s "${_SHELL}" -N -u "${_UID}" "${_USER}"

ENV USER=${_USER}
ENV UID=${_UID}
ENV GID=${_GID}
ENV HOME=/home/${_USER}

# Keep the environment outside /app so the docker-compose bind mount doesn't hide it,
# and use the image's Python rather than downloading one.
ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_PYTHON_DOWNLOADS=never \
    UV_LINK_MODE=copy \
    UV_NO_CACHE=true
ENV PATH="/opt/venv/bin:${HOME}/.local/bin:${PATH}"

RUN mkdir /app && chown ${UID}:${GID} /app
WORKDIR /app

# Install all locked dependency groups. The project itself is not installed: /app is
# bind-mounted at runtime and tools import gbq from the working directory.
COPY ./pyproject.toml ./uv.lock /app/
RUN uv sync --locked --no-install-project

RUN chown -R ${UID}:${GID} /app

USER ${_USER}

CMD bash
