FROM debian:12.4

RUN apt update
RUN apt install -y wget

RUN wget https://download.oracle.com/java/21/latest/jdk-21_linux-x64_bin.tar.gz -O jdk-21.tar.gz
RUN mkdir /usr/lib/jvm && tar zxvf jdk-21.tar.gz --directory /usr/lib/jvm
RUN update-alternatives --install /usr/bin/java java /usr/lib/jvm/jdk-21.*/bin/java 1

COPY libs/*.jar /usr/share/java/responsive-e2etest/
COPY scripts/* /

CMD /run-e2etest
