FROM amd64/debian:11.6

RUN apt update
RUN apt install -y wget

RUN wget https://download.oracle.com/java/21/latest/jdk-21_linux-x64_bin.deb
RUN apt -y install ./jdk-21_linux-x64_bin.deb
RUN update-alternatives --install /usr/bin/java java /usr/lib/jvm/jdk-21-oracle-x64/bin/java 1

COPY libs/*.jar /usr/share/java/responsive-e2etest/
COPY scripts/* /

CMD /run-e2etest
