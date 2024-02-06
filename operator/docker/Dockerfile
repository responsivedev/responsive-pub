FROM debian:12.4 as getjava
LABEL stage=getjava

ARG TARGETPLATFORM

RUN apt update
RUN apt install -y gettext-base
RUN apt install -y wget
RUN echo $TARGETPLATFORM
RUN if [ "$TARGETPLATFORM" = "linux/amd64" ]; then wget https://download.oracle.com/java/21/latest/jdk-21_linux-x64_bin.tar.gz -O jdk-21.tar.gz; fi
RUN if [ "$TARGETPLATFORM" = "linux/arm64" ]; then wget https://download.oracle.com/java/21/latest/jdk-21_linux-aarch64_bin.tar.gz -O jdk-21.tar.gz; fi

FROM debian:12.4
LABEL stage=final

RUN apt update
RUN apt install -y gettext-base

COPY --from=getjava jdk-21.tar.gz jdk-21.tar.gz

RUN mkdir /usr/lib/jvm && tar zxvf jdk-21.tar.gz --directory /usr/lib/jvm
RUN update-alternatives --install /usr/bin/java java /usr/lib/jvm/jdk-21.0.2/bin/java 1

COPY libs/*.jar /usr/share/java/responsive-operator/
COPY scripts/* /

RUN groupadd -g 2000 responsive && useradd -g 2000 responsive-operator
USER responsive-operator:responsive

CMD /run-operator
