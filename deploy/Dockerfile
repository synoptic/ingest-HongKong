ARG FUNCTION_DIR="/function"
FROM --platform=linux/arm64 python:3.13

ARG FUNCTION_DIR
RUN mkdir -p ${FUNCTION_DIR}

# Setup SSH for private git repos
RUN apt-get update -y && apt-get install -y git openssh-client
RUN mkdir -p -m 0600 ~/.ssh && \
    ssh-keyscan -H github.com >> ~/.ssh/known_hosts

WORKDIR ${FUNCTION_DIR}

# Install Lambda runtime interface client
RUN pip install --target ${FUNCTION_DIR} awslambdaric

# Install private dependencies
RUN --mount=type=ssh pip install git+ssh://git@github.com/synoptic/synoptic-logging.git@v1.0.5
RUN --mount=type=ssh pip install "git+ssh://git@github.com/synoptic/ingestlib.git@sensor_sql_gen#egg=ingestlib"

# Install requirements
COPY handlers/requirements.txt ${FUNCTION_DIR}
RUN pip install -r requirements.txt

# Copy application code
COPY config/ ${FUNCTION_DIR}/config/
COPY handlers/ ${FUNCTION_DIR}/handlers/
COPY args.py ${FUNCTION_DIR}/

ENTRYPOINT [ "/usr/local/bin/python", "-m", "awslambdaric" ]
# Override CMD in CDK per handler:
#   obs:  handlers.obs_handler.lambda_handler
#   meta: handlers.meta_handler.lambda_handler
CMD ["handlers.obs_handler.lambda_handler"]
