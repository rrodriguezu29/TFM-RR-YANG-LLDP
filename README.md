# Diseño e implementación de una solución para el descubrimiento, modelado y visualización de topologías de red basada en modelos YANG y el protocolo LLDP

La solución desarrollada automatiza el descubrimiento y seguimiento de una topología de red multivendor mediante modelos YANG y protocolos estándar como gNMI y LLDP.

El escenario de red utiliza dispositivos **Arista cEOS**, que exponen información mediante modelos YANG OpenConfig, y dispositivos **Nokia SR Linux**, que utilizan modelos YANG propios de SR Linux (`srl_nokia-*`). También se incluyen hosts Linux para representar equipos finales dentro de la topología.

La arquitectura incorpora diferentes servicios para el procesamiento y visualización de la información. **Kafka** se utiliza para transportar los mensajes de topología mediante los topics `input`, `output_json` y `output_xml`. **Apache Flink** actúa como plataforma de procesamiento de datos en streaming y ejecuta la aplicación Java `TopologyDriver.java`, encargada de transformar la topología recibida a representaciones normalizadas basadas en modelos YANG. Finalmente, **Flask**, **Socket.IO** y **Cytoscape.js** permiten visualizar la topología desde una interfaz web.

## Flujo de funcionamiento

La siguiente figura muestra el flujo completo de la solución, desde el descubrimiento de la red hasta la visualización de la topología en la interfaz web.

<img width="887" height="718" alt="image" src="https://github.com/user-attachments/assets/9e6f94ec-de6d-46aa-b066-745769d7ee1a" />


### 1. Descubrimiento de la topología

Como punto de partida, `streamneighbor.py` carga el inventario de red desde `ansible-inventory.yml`, generado automáticamente por **Containerlab** durante el despliegue del escenario.

A partir de este inventario, StreamNeighbor utiliza **gNMIc** como cliente gNMI para comunicarse con los dispositivos y realiza dos operaciones principales:

- **gNMI Get:** consulta la información LLDP para obtener las relaciones de vecindad entre los equipos y las interfaces que los conectan. También obtiene información adicional de las interfaces necesaria para construir la topología.
- **gNMI Subscribe `on-change`:** mantiene suscripciones al estado operativo de las interfaces para detectar cambios y actualizar la topología cuando una interfaz pasa de `up` a `down` o viceversa.

Las consultas se realizan sobre las rutas definidas por los modelos YANG implementados por cada fabricante. Los equipos **Arista cEOS** utilizan modelos YANG **OpenConfig**, mientras que **Nokia SR Linux** utiliza modelos YANG nativos `srl_nokia-*`.

Aunque las rutas YANG son diferentes entre fabricantes, StreamNeighbor extrae la información equivalente y la transforma a una representación común de nodos, interfaces y enlaces.

Las respuestas de las operaciones `Get` se solicitan con codificación `JSON_IETF` para facilitar su procesamiento desde Python.

Los hosts Linux participan en el descubrimiento mediante **LLDP**, utilizando el servicio `lldpd`, aunque no son gestionados directamente mediante gNMI.

### 2. Generación de topology.drawio

A partir de los nodos, interfaces, vecinos y estados descubiertos, StreamNeighbor genera:

    topology.drawio

Este archivo contiene una representación gráfica de la topología y puede abrirse mediante diagrams.net/draw.io.

### 3. Generación y publicación de topology.json

StreamNeighbor genera también:

    topology.json

Este archivo contiene la representación de la topología descubierta en formato JSON.

La misma información se publica en **Apache Kafka** utilizando el topic:

    input

Kafka actúa como mecanismo de transporte entre la aplicación de descubrimiento y los componentes encargados del procesamiento y visualización.

### 4. Procesamiento y normalización con Apache Flink

**Apache Flink** consume los mensajes publicados en el topic `input`.

Dentro de Flink se ejecuta la aplicación Java:

    TopologyDriver.java

Esta aplicación parsea el JSON recibido y utiliza **YANG Tools** para mapear la información de la topología a una representación basada en los modelos YANG `ietf-network` e `ietf-network-topology` de RFC 8345.

Durante este proceso, los dispositivos se representan como `nodos`, las interfaces como `TerminationPoint` y las conexiones entre interfaces como `Link`.

### 5. Publicación de las salidas normalizadas

Después del procesamiento, Flink publica las representaciones normalizadas nuevamente en Kafka mediante los topics:

    output_json
    output_xml

`output_json` contiene la representación normalizada en JSON y `output_xml` proporciona una representación alternativa en XML.

### 6. Servidor web

El servidor desarrollado con **Flask** consume información de Kafka.

Para la visualización se utilizan:

    input
    output_json

De esta forma, la interfaz permite disponer tanto de la topología generada originalmente por StreamNeighbor como de la representación normalizada obtenida mediante Apache Flink.

### 7. Visualización con Cytoscape.js

Flask envía las actualizaciones hacia el navegador mediante **Socket.IO**.

En el cliente web, **Cytoscape.js** utiliza estos datos para construir y actualizar dinámicamente los grafos de la topología.

La interfaz permite visualizar:

- La topología generada por StreamNeighbor a partir del topic `input`.
- La topología normalizada según RFC 8345 a partir del topic `output_json`.

Una vez desplegados los servicios y ejecutado StreamNeighbor, la interfaz puede consultarse desde:

    http://localhost:5000/


## Instalación

Los siguientes pasos corresponden a la preparación inicial del escenario antes de realizar el despliegue.

### Imagen Arista cEOS

La imagen de Arista cEOS debe descargarse desde el portal oficial de Arista:

https://www.arista.com/en/support/software-download

Para realizar la descarga es necesario crear previamente una cuenta en el portal de Arista.

Para este escenario se utiliza:

    cEOS64-lab-4.34.5M.tar

Crear una carpeta `img` en la raíz del proyecto y copiar en ella la imagen descargada.

Acceder al directorio:

    cd img

Importar la imagen en Docker:

    docker import cEOS64-lab-4.34.5M.tar ceos:4.34.5M

### Imagen Ubuntu

Acceder al directorio que contiene el Dockerfile:

    cd docker/

Construir la imagen:

    docker build -t ubuntu-lldp:latest .

También puede construirse utilizando el nombre empleado por el escenario:

    sudo docker build -t giros-dit/clab-telemetry-testbed-ubuntu:latest .

Crear la etiqueta:

    sudo docker tag giros-dit/clab-telemetry-testbed-ubuntu:latest ubuntu-lldp:latest

### Dependencias

Instalar las dependencias de Python:

    python3 -m pip install kafka-python
    python3 -m pip install pyyaml deepdiff

Instalar Maven:

    sudo apt update
    sudo apt install -y maven

## Despliegue del escenario de red

Desde la raíz del repositorio acceder al directorio:

    cd routing-testbed-ceos-and-srlinux

Desplegar el escenario:

    ./deploy-routing-testbed.sh

Configurar el direccionamiento y el enrutamiento:

    ./configure-addressing-and-routing.sh

Una vez desplegado el laboratorio puede visualizarse la topología creada mediante Containerlab, como la siguiente:

<img width="865" height="390" alt="image" src="https://github.com/user-attachments/assets/48690191-5ff3-4822-ba76-f1a5ad39edb2" />


## Servicios Docker

Regresar a la raíz del repositorio y acceder al directorio:

    cd ../topology-discoverer

Levantar los servicios:

    docker compose up

En otra terminal, comprobar que los contenedores estén activos:

    docker ps

## Apache Flink

Desde el directorio `topology-discoverer`, desplegar el job:

    ./deploy_flink.sh

El script envía al **JobManager de Flink** el archivo JAR que contiene la aplicación `TopologyDriver.java`. Esta consume la topología del topic `input`, la normaliza utilizando los modelos YANG de **RFC 8345** y publica los resultados en los topics `output_json` y `output_xml`.

## Ejecución de StreamNeighbor

Regresar a la raíz del repositorio:

    cd ..

Ejecutar la aplicación Python:

    python streamneighbor.py

StreamNeighbor realiza el descubrimiento inicial de la topología mediante gNMI y LLDP y mantiene suscripciones gNMI `Subscribe on-change` para detectar cambios en el estado operativo de las interfaces.

Una vez iniciada la aplicación se generan los siguientes archivos:

    topology.json
    topology.drawio

`topology.json` contiene la representación de la topología descubierta y `topology.drawio` permite visualizar la topología mediante diagrams.net/draw.io.

## Visualización web

Una vez levantados los servicios y ejecutada la aplicación StreamNeighbor, acceder desde el navegador a:

http://localhost:5000/

La interfaz web permite visualizar tanto la topología recibida desde StreamNeighbor (StreamNeighbor Topology) como la representación normalizada procesada por Apache Flink (RFC 8345 Topology).

## Orden de ejecución

    cd routing-testbed-ceos-and-srlinux

    ./deploy-routing-testbed.sh
    ./configure-addressing-and-routing.sh

    cd ../topology-discoverer

    docker compose up

En otra terminal:

    docker ps

Desde `topology-discoverer`:

    ./deploy_flink.sh

Finalmente, desde la raíz del repositorio:

    cd ..

    python streamneighbor.py

Acceder desde el navegador a:

    http://localhost:5000/

## Destruir el escenario

Acceder nuevamente al directorio del escenario:

    cd routing-testbed-ceos-and-srlinux

Ejecutar:

    ./destroy-routing-testbed.sh
