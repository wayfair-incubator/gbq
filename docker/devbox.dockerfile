FROM python:3.15-bookworm@sha256:6f060cab421d9509c6c6a6a7762c651baf03c3f1d0145befe16b688762b7fe81

# Pinned uv binary, copied from the official image instead of piping an install script to sh
COPY --from=ghcr.io/astral-sh/uv:0.13.8@sha256:5cc1805e2426484abca4ad46ea783bab281fdb225fd82d8ce6d66a81254bac85 /uv /uvx /usr/local/bin/

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
