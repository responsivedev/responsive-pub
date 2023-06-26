FROM amd64/debian:11.6 

RUN apt update
RUN apt install -y gettext-base
RUN apt install -y wget
RUN wget https://download.oracle.com/java/20/latest/jdk-20_linux-x64_bin.deb
RUN apt -y install ./jdk-20_linux-x64_bin.deb
RUN update-alternatives --install /usr/bin/java java /usr/lib/jvm/jdk-20/bin/java 1

COPY libs/*.jar /usr/share/java/responsive-operator/
COPY scripts/* /

CMD /run-operator
