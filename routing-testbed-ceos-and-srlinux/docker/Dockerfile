# Dockerfile to build a custom Ubuntu image to be used in the topology

# Base image is Ubuntu 20.04 LTS (bionic)
FROM ubuntu:bionic

# Evita prompts interactivos
ENV DEBIAN_FRONTEND=noninteractive

# Preinstalled software and packages are updated
RUN apt-get update && apt-get -y dist-upgrade && apt-get -y autoremove && apt-get autoclean

# Other packages are installed
RUN apt-get install -y iproute2 iputils-ping net-tools ldnsutils traceroute iperf3 tcpdump openssh-server nmap curl wget lldpd

# Habilitar LLDP en todas las interfaces
RUN echo 'DAEMON_ARGS="-I eth*"' > /etc/default/lldpd

# Comando por defecto: arrancar lldpd y bash
CMD service lldpd start && bash