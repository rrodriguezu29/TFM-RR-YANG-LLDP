# TFM-RR-YANG-LLDP
# StreamNeighbor

Guía para instalar las dependencias, desplegar el escenario de red y ejecutar StreamNeighbor.

## Instalación

### Imágenes Docker

Acceder al directorio de imágenes:

```
cd /home/upm/Desktop/yang-lab/activities/module-TFM/img
```

Importar la imagen de Arista cEOS:

```
docker import cEOS64-lab-4.34.5M.tar ceos:4.34.5M
```

Acceder al directorio Docker:

```
cd docker/
```

Construir la imagen Ubuntu con LLDP:

```
docker build -t ubuntu-lldp:latest .
```

Construir y etiquetar la imagen utilizada por el escenario:

```
sudo docker build -t giros-dit/clab-telemetry-testbed-ubuntu:latest .
sudo docker tag giros-dit/clab-telemetry-testbed-ubuntu:latest ubuntu-lldp:latest
```

### Validar LLDP

Ejecutar un contenedor temporal:

```
docker run -it --rm ubuntu-lldp:latest bash
```

Dentro del contenedor, validar la instalación de `lldpd`:

```
which lldpd
lldpd -v
```

### Dependencias

Instalar las dependencias de Python:

```
python3 -m pip install kafka-python
python3 -m pip install pyyaml deepdiff
```

Instalar Maven:

```
sudo apt update
sudo apt install -y maven
```

## Despliegue del escenario de red

Acceder al directorio del escenario:

```
cd /home/upm/Desktop/yang-lab/activities/module-TFM/routing-testbed-ceos-and-srlinux
```

Desplegar el escenario:

```
./deploy-routing-testbed.sh
```

Configurar direccionamiento y routing:

```
./configure-addressing-and-routing.sh
```

## Servicios Docker

Acceder al directorio `topology-discoverer`:

```
cd /home/upm/Desktop/yang-lab/activities/module-TFM/topology-discoverer
```

Levantar los servicios:

```
docker compose up
```

En otra terminal, validar que los contenedores estén activos:

```
docker ps
```

## Apache Flink

Desde el directorio `topology-discoverer`, lanzar el job de Flink:

```
./deploy_flink.sh
```

## StreamNeighbor

Acceder al directorio principal:

```
cd /home/upm/Desktop/yang-lab/activities/module-TFM
```

Ejecutar StreamNeighbor:

```
python streamneighbor.py
```

## Orden de ejecución

```
1. ./deploy-routing-testbed.sh
2. ./configure-addressing-and-routing.sh
3. docker compose up
4. docker ps
5. ./deploy_flink.sh
6. python streamneighbor.py
```

## Destruir el escenario

Acceder al directorio del escenario:

```
cd /home/upm/Desktop/yang-lab/activities/module-TFM/routing-testbed-ceos-and-srlinux
```

Ejecutar:

```
./destroy-routing-testbed.sh
```
