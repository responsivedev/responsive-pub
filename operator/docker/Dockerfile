FROM debian:11.6

ARG TARGETPLATFORM

RUN apt update
RUN apt install -y gettext-base
RUN apt install -y wget
RUN echo $TARGETPLATFORM
RUN if [ "$TARGETPLATFORM" = "linux/amd64" ]; then wget https://download.oracle.com/java/20/latest/jdk-20_linux-x64_bin.tar.gz -O jdk-20.tar.gz; fi
RUN if [ "$TARGETPLATFORM" = "linux/arm64" ]; then wget https://download.oracle.com/java/20/latest/jdk-20_linux-aarch64_bin.tar.gz -O jdk-20.tar.gz; fi
RUN mkdir /usr/lib/jvm && tar zxvf jdk-20.tar.gz --directory /usr/lib/jvm
RUN update-alternatives --install /usr/bin/java java /usr/lib/jvm/jdk-20.0.2/bin/java 1

COPY libs/*.jar /usr/share/java/responsive-operator/
COPY scripts/* /

CMD /run-operator
