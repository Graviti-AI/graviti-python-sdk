From ubuntu:20.04

RUN apt-get update && \
    apt-get -y --no-install-recommends install \
        git=1:2.25.1-1ubuntu3.5 \
        python3.8=3.8.10-0ubuntu1~20.04.5 \
        python3-pip=20.0.2-5ubuntu1.6 && \
    rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir \
        pandas==1.5.0 \
        pillow==9.2.0

RUN pip3 install --no-cache-dir \
        graviti==0.10.2

ENV PORTEX_STANDARD_URL https://github.com/Project-OpenBytes/portex-standard
RUN echo "import graviti.portex as pt\npt.build_package('$PORTEX_STANDARD_URL', 'main')" | python3
