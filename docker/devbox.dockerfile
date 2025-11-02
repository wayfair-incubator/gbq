FROM python:3.14-bookworm

ARG _USER="gbq"
ARG _UID="1000"
ARG _GID="100"
ARG _SHELL="/bin/bash"

# Install uv as root before creating the user
ENV UV_NO_CACHE="true"
RUN curl -LsSf https://astral.sh/uv/install.sh | sh && \
    cp /root/.local/bin/uv /usr/local/bin/uv && \
    cp /root/.local/bin/uvx /usr/local/bin/uvx

RUN useradd -m -s "${_SHELL}" -N -u "${_UID}" "${_USER}"

ENV USER=${_USER}
ENV UID=${_UID}
ENV GID=${_GID}
ENV HOME=/home/${_USER}
ENV PATH="${HOME}/.local/bin/:${PATH}"

RUN mkdir /app && chown ${UID}:${GID} /app

# Copy requirements files as root since we'll install as root
COPY ./requirements* /app/
WORKDIR /app

# Install from requirements.txt first, then add test and docs requirements
# Skip lock file if it's empty or just a comment
# Install as root since --system requires write access to system site-packages
RUN if [ -s requirements.lock ] && [ "$(grep -v '^#' requirements.lock | wc -l)" -gt 0 ]; then \
        uv pip install --system -r requirements.lock -r requirements-test.txt -r requirements-docs.txt; \
    else \
        uv pip install --system -r requirements.txt -r requirements-test.txt -r requirements-docs.txt; \
    fi

# Change ownership of app directory to user
RUN chown -R ${UID}:${GID} /app

USER ${_USER}

CMD bash
