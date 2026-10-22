FROM python:3.15-bookworm@sha256:2734c1683bd937b316f31b18972026302f155f0ffd932b329811d2e3a8450775

# Pinned uv binary, copied from the official image instead of piping an install script to sh
COPY --from=ghcr.io/astral-sh/uv:0.13.5@sha256:32a7bb39b496babbd997b0a39a4d66c126bdaf5661b302bf6f06abda8deca6ff /uv /uvx /usr/local/bin/

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
