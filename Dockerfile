FROM python:3.13

LABEL version="1.0"
LABEL maintainer="remi.venant@univ-lemans.fr"
LABEL description="SAE301/303 2024-2025: Master Stats"

WORKDIR /usr/src/app

# Install python requirements
COPY ./requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code to allow running the container without mounting the source code as a volume
COPY . .

# Precompile python file
RUN python -m compileall .

# Entrypoint defined with an array to allow provide argument with CMD command
ENTRYPOINT [ "/bin/sh", "./start_server.sh" ]