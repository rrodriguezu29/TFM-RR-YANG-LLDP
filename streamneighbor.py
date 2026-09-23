import json
import subprocess
import time
import re
from xml.etree.ElementTree import Element, SubElement, tostring
import yaml
import fcntl
import os
import threading
from kafka import KafkaProducer


KAFKA_BOOTSTRAP = "localhost:9094"
KAFKA_TOPIC_INPUT = "input"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8")
)

INVENTORY_FILE = "routing-testbed-ceos-and-srlinux/clab-routing-testbed-ceos-and-srlinux/ansible-inventory.yml"


VENDOR_GROUPS = {
    "nokia": {
        "group_names": ["nokia_srlinux"],
        "port": 57400,
        "default_user": "admin",
        "default_password": "NokiaSrl1!",
    },
    "arista": {
        "group_names": ["arista_ceos","ceos"],
        "port": 6030,
        "default_user": "admin",
        "default_password": "admin",
    },
}

PATHS = {
    "nokia": {
        "lldp_interfaces": "/srl_nokia-system:system/srl_nokia-lldp:lldp/interface",
        "lldp_neighbors_stream": "/srl_nokia-system:system/srl_nokia-lldp:lldp/interface/neighbor/id",
        "mgmt_ip": "/interface[name=mgmt0]/subinterface/ipv4/address/ip-prefix",
        "interfaces_ip": "/interface[name=*]/subinterface/ipv4/address/ip-prefix",
        "if_state": "/interface[name={interface}]",
        "if_state_stream": "/interface/subinterface/oper-state",
        "host-name": "/system/name/host-name",
        "subif_admin_state": "/interface[name={interface}]/subinterface[index=0]/admin-state",
        "subif_oper_state": "/interface[name={interface}]/subinterface[index=0]/oper-state",
    },
    "arista": {
        "lldp_interfaces": "/lldp/interfaces/interface",
        "lldp_neighbors_stream": "/lldp/interfaces/interface/neighbors/neighbor",
        "interfaces": "/interfaces/interface",
        "mgmt_ip": "/interfaces/interface[name=Management0]/subinterfaces/subinterface/ipv4/addresses/address/ip",
        "interfaces_ip": "/interfaces/interface/subinterfaces/subinterface/ipv4/addresses/address/ip",
        "if_state": "/interfaces/interface[name={interface}]/state",
        "if_state_stream": "/interfaces/interface/state/oper-status",
        "host-name": "/system/state/hostname",
    },
}


def run_cmd(cmd):
    return subprocess.check_output(cmd, text=True, timeout=5)


def load_devices_from_inventory(path=INVENTORY_FILE):
    with open(path, "r", encoding="utf-8") as f:
        inv = yaml.safe_load(f)


    children = inv.get("all", {}).get("children", {})
    devices = []
    node_ip = {}


    for vendor, meta in VENDOR_GROUPS.items():
        for group_name in meta["group_names"]:
            group = children.get(group_name)
            if not group:
                continue

            hosts = group.get("hosts", {})
            vars_ = group.get("vars", {})

            user = vars_.get("ansible_user", meta["default_user"])
            password = vars_.get("ansible_password", meta["default_password"])
            port = meta["port"]

            for hostname, data in hosts.items():
                ip = data.get("ansible_host", hostname)
                alias = hostname.split("-")[-1]

                devices.append(
                    {
                        "name": alias,
                        "container": hostname,
                        "addr": f"{ip}:{port}",
                        "vendor": vendor,
                        "user": user,
                        "password": password,
                        "mgmt_ip": ip,
                    }
                )
                node_ip[alias] = ip

    return devices, node_ip


DEVICES, NODE_IP = load_devices_from_inventory()

def get_device_by_name(name):
    for d in DEVICES:
        if d["name"] == name:
            return d
    return None


def gnmic_get(device, path):
    cmd = [
        "gnmic", "-a", device["addr"],
        "-u", device["user"], "-p", device["password"],
    ]

    if device["vendor"] == "arista":
        cmd.append("--insecure")
    else:
        cmd.append("--skip-verify")

    cmd += [
        "get",
        "--path", path,
        "--encoding", "json_ietf",
        "--format", "json"
    ]

    out = run_cmd(cmd)
    return json.loads(out)

def check_device_status(device):
    try:
        path = PATHS[device["vendor"]]["host-name"]
        gnmic_get(device, path)
        return "up"

    except Exception as e:
        return "down"

def find_ip_prefix(obj):
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == "ip-prefix" and isinstance(v, str):
                return v
            result = find_ip_prefix(v)
            if result:
                return result
    elif isinstance(obj, list):
        for item in obj:
            result = find_ip_prefix(item)
            if result:
                return result
    return None


def find_key_endswith(obj, suffix):
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k.endswith(suffix):
                return v
            result = find_key_endswith(v, suffix)
            if result is not None:
                return result
    elif isinstance(obj, list):
        for item in obj:
            result = find_key_endswith(item, suffix)
            if result is not None:
                return result
    return None


def get_nokia_mgmt_ip(device):
    path = PATHS["nokia"]["mgmt_ip"]
    payload = gnmic_get(device, path)
    ip_prefix = find_ip_prefix(payload)
    if ip_prefix:
        return ip_prefix.split("/")[0]
    return None

def get_arista_mgmt_ip(device):
    path = PATHS["arista"]["mgmt_ip"]
    payload = gnmic_get(device, path)

    ip = find_key_endswith(payload, "address/ip")
    if ip and isinstance(ip, str):
        return ip

    return None


def get_mgmt_ip(device):
    if device["vendor"] == "nokia":
        return get_nokia_mgmt_ip(device)
    elif device["vendor"] == "arista":
        return get_arista_mgmt_ip(device)
    return None


def load_node_ips_from_gnmi():
    for device in DEVICES:
        ip = get_mgmt_ip(device)
        if ip:
            NODE_IP[device["name"]] = ip


def normalize_remote_name(name):
    if not name:
        return None
    if name.startswith("clab-"):
        return name.split("-")[-1]
    return name


def get_interface_state(device, interface_name):
    try:
        path_template = PATHS[device["vendor"]]["if_state"]
        path = path_template.format(interface=interface_name)
        payload = gnmic_get(device, path)

        if device["vendor"] == "arista":
            admin = find_key_endswith(payload, "admin-status")
            oper = find_key_endswith(payload, "oper-status")
            admin_ok = str(admin).upper() in ("UP", "TRUE", "ENABLED")
            oper_ok = str(oper).upper() == "UP"
            return admin_ok and oper_ok

        elif device["vendor"] == "nokia":
            admin = find_key_endswith(payload, "admin-state")
            oper = find_key_endswith(payload, "oper-state")
            admin_ok = str(admin).lower() == "enable"
            oper_ok = str(oper).lower() == "up"
            return admin_ok and oper_ok

    except subprocess.CalledProcessError:
        return False
    except Exception:
        return False

    return False


def get_nokia_topology_interface_state(device, interface_name):
    """
    Para Nokia:
    - Si existe subinterface 0, utiliza su admin-state y oper-state.
    - Si no existe subinterface 0, utiliza el estado de la interfaz física.
    """
    subif_admin_path = (
        f"/interface[name={interface_name}]"
        f"/subinterface[index=0]/admin-state"
    )
    subif_oper_path = (
        f"/interface[name={interface_name}]"
        f"/subinterface[index=0]/oper-state"
    )
    try:
        admin_payload = gnmic_get(device, subif_admin_path)
        oper_payload = gnmic_get(device, subif_oper_path)
        admin = find_key_endswith(admin_payload, "admin-state")
        oper = find_key_endswith(oper_payload, "oper-state")
        # Si existen ambos valores, la subinterface 0 existe
        if admin is not None and oper is not None:
            admin_ok = str(admin).lower() == "enable"
            oper_ok = str(oper).lower() == "up"
            return admin_ok and oper_ok
    except Exception:
        # Si no existe la subinterface 0,
        # se comprobará la interfaz física.
        pass
    physical_admin_path = (
        f"/interface[name={interface_name}]/admin-state"
    )
    physical_oper_path = (
        f"/interface[name={interface_name}]/oper-state"
    )
    try:
        admin_payload = gnmic_get(device, physical_admin_path)
        oper_payload = gnmic_get(device, physical_oper_path)
        admin = find_key_endswith(admin_payload, "admin-state")
        oper = find_key_endswith(oper_payload, "oper-state")
        admin_ok = str(admin).lower() == "enable"
        oper_ok = str(oper).lower() == "up"
        return admin_ok and oper_ok
    except Exception as e:
        return False


def get_current_interface_status(node, interface):
    key = (node, interface)
    if key in INTERFACE_STATUS_EVENTS:
        return INTERFACE_STATUS_EVENTS[key]
    device = next(
        (device for device in DEVICES if device["name"] == node),
        None
    )
    if device is None:
        return None

    if not interface or interface == "?":
        return None
    if device["vendor"] == "nokia":
        interface_up = get_nokia_topology_interface_state( device, interface )
    # Arista mantiene logica anterior.
    else:
        interface_up = get_interface_state( device, interface )
    if interface_up:
        return "up"
    return "down"


#problem rtr like r12 <= r2 true compare lexicografica
def build_edge(local, local_if, remote, remote_if):
    if local <= remote:
        key = (local, remote)
        data = {
            "left_interface": local_if,
            "right_interface": remote_if,
        }
    else:
        key = (remote, local)
        data = {
            "left_interface": remote_if,
            "right_interface": local_if,
        }
    return key, data

def looks_like_mac(value):
    if not isinstance(value, str):
        return False

    value = value.strip().replace('"', '')
    return value.count(":") == 5

def get_remote_interface(port_id, port_description):
    port_id = port_id.strip().replace('"', '') if isinstance(port_id, str) else ""
    port_description = port_description.strip().replace('"', '') if isinstance(port_description, str) else ""

    if port_id and not looks_like_mac(port_id):
        return port_id

    if port_description:
        return port_description

    if port_id:
        return port_id

    return "?"


def edges_from_nokia(payload, local):
    responses = payload if isinstance(payload, list) else [payload]
    edges_map = {}

    for resp in responses:
        if not isinstance(resp, dict):
            continue

        updates = resp.get("updates", [])
        for upd in updates:
            values = upd.get("values", {})
            if not isinstance(values, dict):
                continue

            for value in values.values():
                if not isinstance(value, dict):
                    continue

                interfaces = value.get("interface", [])
                for itf in interfaces:
                    local_if = itf.get("name", "")
                    if not local_if or local_if.startswith("mgmt"):
                        continue

                    neighbors = itf.get("neighbor", [])
                    for nb in neighbors:
                        remote = nb.get("system-name")
                        if not remote:
                            continue

                        remote_if = get_remote_interface(
                            nb.get("port-id"),
                            nb.get("port-description")
                        )

                        local_device = get_device_by_name(local)
                        remote_device = get_device_by_name(remote)

                        if local_device and not get_interface_state(local_device, local_if):
                            continue

                        if remote_device and remote_if != "?" and not looks_like_mac(remote_if):
                            if not get_interface_state(remote_device, remote_if):
                                continue

                        key, edge_data = build_edge(local, local_if, remote, remote_if)

                        if key not in edges_map:
                            edges_map[key] = edge_data

    return edges_map


def get_arista_local_if(value):
    return (
        value.get("openconfig-lldp:name")
        or value.get("openconfig-lldp:config", {}).get("name")
        or "?"
    )


def get_arista_neighbors(value):
    neighbors_block = value.get("openconfig-lldp:neighbors", {})
    if isinstance(neighbors_block, dict):
        return neighbors_block.get("neighbor", [])
    return []


def edges_from_arista(payload, local):
    responses = payload if isinstance(payload, list) else [payload]
    edges_map = {}

    for resp in responses:
        if not isinstance(resp, dict):
            continue

        updates = resp.get("updates", [])
        for upd in updates:
            values = upd.get("values", {})
            if not isinstance(values, dict):
                continue

            for value in values.values():
                if not isinstance(value, dict):
                    continue

                local_if = get_arista_local_if(value)

                if not local_if or str(local_if).lower().startswith("management"):
                    continue

                neighbors = get_arista_neighbors(value)

                for nb in neighbors:
                    if not isinstance(nb, dict):
                        continue

                    state = nb.get("state", {}) if isinstance(nb.get("state"), dict) else {}

                    remote = state.get("system-name") or nb.get("id")
                    if not remote:
                        continue

                    remote_if = get_remote_interface(
                        state.get("port-id"),
                        state.get("port-description")
                    )

                    local_device = get_device_by_name(local)
                    remote_device = get_device_by_name(remote)

                    if local_device and not get_interface_state(local_device, local_if):
                        continue

                    if remote_device and remote_if != "?" and not looks_like_mac(remote_if):
                        if not get_interface_state(remote_device, remote_if):
                            continue

                    key, edge_data = build_edge(local, local_if, remote, remote_if)

                    if key not in edges_map:
                        edges_map[key] = edge_data

    return edges_map


def collect_edges(device):
    path = PATHS[device["vendor"]]["lldp_interfaces"]
    payload = gnmic_get(device, path)

    if device["vendor"] == "nokia":
        return edges_from_nokia(payload, device["name"])

    if device["vendor"] == "arista":
        return edges_from_arista(payload, device["name"])

    return {}


def get_nokia_interfaces_ip_map(device):
    path = PATHS["nokia"]["interfaces_ip"]
    payload = gnmic_get(device, path)

    ip_map = {}
    responses = payload if isinstance(payload, list) else [payload]

    for resp in responses:
        if not isinstance(resp, dict):
            continue

        for upd in resp.get("updates", []):
            values = upd.get("values", {})
            if not isinstance(values, dict):
                continue

            for root in values.values():
                if not isinstance(root, dict):
                    continue

                interfaces = root.get("srl_nokia-interfaces:interface", [])
                for itf in interfaces:
                    if_name = itf.get("name")
                    if not if_name:
                        continue

                    for subif in itf.get("subinterface", []):
                        ipv4 = subif.get("ipv4", {})
                        for addr in ipv4.get("address", []):
                            ip_prefix = addr.get("ip-prefix")
                            if isinstance(ip_prefix, str):
                                ip_map[if_name] = ip_prefix
                                break

    return ip_map


def get_arista_interfaces_ip_map(device):
    path = PATHS["arista"]["interfaces_ip"]
    payload = gnmic_get(device, path)

    ip_map = {}
    responses = payload if isinstance(payload, list) else [payload]

    for resp in responses:
        for upd in resp.get("updates", []):
            path_str = upd.get("Path", "")
            values = upd.get("values", {})

            ip = next(iter(values.values()), None)
            if not isinstance(ip, str):
                continue

            m = re.search(r"interface\[name=([^\]]+)\]", path_str)
            if not m:
                continue

            if_name = m.group(1)
            ip_map[if_name] = ip

    return ip_map


def get_host_ip(host_name):
    container_name = f"clab-routing-testbed-ceos-and-srlinux-{host_name}"
    try:
        cmd = [
            "docker", "exec", container_name,
            "sh", "-c",  "ip -4 addr show eth1 | awk '/inet / {print $2}' | cut -d/ -f1"
        ]
        ip = subprocess.check_output(cmd, text=True, timeout=3).strip()
        if ip:
            return ip
    except Exception as e:
        print(f"[warn] no se pudo obtener IP de {host_name}: {e}")
    return "N/A"


def build_host_topology_json(host_name, edges_map):
    host_ip = get_host_ip(host_name)
    result = {
        "container": host_name,
        "interfaces": [],
        "mgmt_interface": "N/A",
        "vendor": "host"
    }

    for (a, b), edge_data in edges_map.items():
        if host_name not in (a, b):
            continue

        if host_name == a:
            local_if = edge_data["left_interface"]
            remote_node = b
            remote_if = edge_data["right_interface"]
        else:
            local_if = edge_data["right_interface"]
            remote_node = a
            remote_if = edge_data["left_interface"]

        result["interfaces"].append({
            "name": local_if,
            "ip": host_ip,
            "adjacent-node": remote_node,
            "adjacent-interface": remote_if,
            "termination-point": f"{remote_node}:{remote_if}",
            "status": edge_data.get("status", "unknown")
        })

    return result


def node_status(node, edges_map):
    return (
        "DOWN"
        if any(
            node in k and v.get("status") == "down"
            for k, v in edges_map.items()
        )
        else "UP"
    )

def write_drawio(nodes, edges_map, node_status_map, interface_ips, out="topology.drawio"):
    mxfile = Element(
        "mxfile",
        host="app.diagrams.net",
        modified=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    )
    diagram = SubElement(mxfile, "diagram", name="LLDP")
    model = SubElement(diagram, "mxGraphModel")
    root = SubElement(model, "root")

    SubElement(root, "mxCell", id="0")
    SubElement(root, "mxCell", id="1", parent="0")

    ids = {}
    i = 2

    for idx, n in enumerate(sorted(nodes)):
        ids[n] = str(i)
        i += 1

        node_label = f"{n} ({NODE_IP[n]})" if n in NODE_IP else n
        if n.lower().startswith("pc"):

            border_color = "#666666"
            fill_color = "#FFFFFF"
            stroke_width = "1"

        else:
            node_status = node_status_map.get(n, "down")

            if node_status == "up":
                border_color = "#B0CFB0"
                fill_color = "#FFFFFF"
            else:
                border_color = "#FF0000"
                fill_color = "#FFFFFF"
            stroke_width = "3"

        cell = SubElement(
            root,
            "mxCell",
            id=ids[n],
            value=node_label,
            vertex="1",
            parent="1",
            style=(
                f"rounded=1;"
                f"whiteSpace=wrap;"
                f"html=1;"
                f"strokeColor={border_color};"
                f"fillColor={fill_color};"
                f"strokeWidth={stroke_width};"
            )
        )

        x = (idx % 4) * 220 + 40
        y = (idx // 4) * 140 + 40

        SubElement(
            cell,
            "mxGeometry",
            x=str(x),
            y=str(y),
            width="150",
            height="60",
            as_="geometry"
        )

    for (a, b), edge_data in sorted(edges_map.items()):
        label = f"{edge_data['left_interface']} - {edge_data['right_interface']}"

        status = edge_data.get("status", "unknown")

        if status == "up":
            line_color = "#00AA00"
        else:
            line_color = "#FF0000"

        e = SubElement(
            root,
            "mxCell",
            id=str(i),
            value=label,
            edge="1",
            parent="1",
            source=ids[a],
            target=ids[b],
            style=f"endArrow=none;html=1;whiteSpace=wrap;endFill=0;strokeColor={line_color};strokeWidth=3;"


        )
        i += 1

        SubElement(
            e,
            "mxGeometry",
            relative="1",
            as_="geometry"
        )


    table_x = 900
    table_y = 40
    col_widths = [80, 120, 110, 70]
    row_h = 28

    rows = []
    rows.append(["Equipo", "Puerto", "IP", "Status"])
    seen = set()
    for (a, b), edge_data in sorted(edges_map.items()):
        status = edge_data.get("status", "unknown").upper()

        left_interface = edge_data.get("left_interface", "N/A")
        right_interface = edge_data.get("right_interface", "N/A")


        ip_a = interface_ips.get(a, {}).get(left_interface, NODE_IP.get(a, "N/A"))
        ip_b = interface_ips.get(b, {}).get(right_interface, NODE_IP.get(b, "N/A"))

        row_a = (a, left_interface, ip_a, status)
        row_b = (b, right_interface, ip_b, status)

        if row_a not in seen:
            rows.append(list(row_a))
            seen.add(row_a)

        if row_b not in seen:
            rows.append(list(row_b))
            seen.add(row_b)

    table_width = sum(col_widths)
    table_height = row_h * len(rows)

    table_id = str(i)
    i += 1

    table = SubElement(
        root,
        "mxCell",
        id=table_id,
        value="",
        vertex="1",
        parent="1",
        style=(
            "shape=table;"
            "childLayout=tableLayout;"
            "startSize=0;"
            "collapsible=0;"
            "recursiveResize=0;"
            "strokeColor=#98bf21;"
            "fillColor=#A7C942;"
            "shadow=1;"
            "html=1;"
        )
    )

    SubElement(
        table,
        "mxGeometry",
        x=str(table_x),
        y=str(table_y),
        width=str(table_width),
        height=str(table_height),
        as_="geometry"
    )

    for r, row in enumerate(rows):
        row_id = str(i)
        i += 1

        row_cell = SubElement(
            root,
            "mxCell",
            id=row_id,
            value="",
            vertex="1",
            parent=table_id,
            style=(
                "shape=tableRow;"
                "horizontal=0;"
                "startSize=0;"
                "swimlaneHead=0;"
                "swimlaneBody=0;"
                "fillColor=none;"
                "collapsible=0;"
                "dropTarget=0;"
                "points=[];"
                "portConstraint=eastwest;"
                "top=0;"
                "left=0;"
                "right=0;"
                "bottom=0;"
            )
        )

        SubElement(
            row_cell,
            "mxGeometry",
            y=str(r * row_h),
            width=str(table_width),
            height=str(row_h),
            as_="geometry"
        )

        x_pos = 0

        for c, text in enumerate(row):
            cell_id = str(i)
            i += 1

            if r == 0:
                fill = "#A7C942"
                font_color = "#FFFFFF"
                font_style = "1"
            else:
                fill = "#FFFFFF"
                font_color = "#000000"
                font_style = "0"

            if c == 3 and r > 0:
                if text == "UP":
                    font_color = "#008000"
                    font_style = "1"
                else:
                    font_color = "#FF0000"
                    font_style = "1"

            data_cell = SubElement(
                root,
                "mxCell",
                id=cell_id,
                value=text,
                vertex="1",
                parent=row_id,
                style=(
                    "shape=partialRectangle;"
                    "connectable=0;"
                    "fillColor=" + fill + ";"
                    "strokeColor=#98bf21;"
                    "fontColor=" + font_color + ";"
                    "fontStyle=" + font_style + ";"
                    "align=center;"
                    "verticalAlign=middle;"
                    "whiteSpace=wrap;"
                    "html=1;"
                    "overflow=hidden;"
                )
            )

            SubElement(
                data_cell,
                "mxGeometry",
                x=str(x_pos),
                width=str(col_widths[c]),
                height=str(row_h),
                as_="geometry"
            )

            x_pos += col_widths[c]

    xml = tostring(mxfile, encoding="utf-8").decode()
    xml = xml.replace('as_="geometry"', 'as="geometry"')

    with open(out, "w", encoding="utf-8") as f:
        f.write(xml)


def send_topology_to_kafka(topology, topic=KAFKA_TOPIC_INPUT):
    producer.send(topic, topology)
    producer.flush()


def build_device_topology_json(device, edges_map, node_ip, interface_ip_map=None):
    device_name = device["name"]

    if interface_ip_map is None:
        interface_ip_map = {}

    mgmt_name = "mgmt0" if device["vendor"] == "nokia" else "Management0"

    result = {
        "container": device_name,
        "interfaces": [],
        "mgmt_interface": mgmt_name,
        "vendor": device["vendor"],
    }

    for (a, b), edge_data in edges_map.items():
        if device_name not in (a, b):
            continue

        if device_name == a:
            local_if = edge_data["left_interface"]
            remote_node = b
            remote_if = edge_data["right_interface"]
        else:
            local_if = edge_data["right_interface"]
            remote_node = a
            remote_if = edge_data["left_interface"]

        result["interfaces"].append({
            "name": local_if,
            "ip": interface_ip_map.get(local_if, "N/A"),
            "adjacent-node": remote_node,
            "adjacent-interface": remote_if,
            "termination-point": f"{remote_node}:{remote_if}",
            "status": edge_data.get("status", "unknown")
        })

    mgmt_ip = (
        interface_ip_map.get(mgmt_name)
        or node_ip.get(device_name)
        or device.get("mgmt_ip", "NF")
    )

    result["interfaces"].append({
        "name": mgmt_name,
        "ip": mgmt_ip
    })

    return result

def build_topology_json(devices, edges_map, node_ip):
    topology = []
    known_devices = set()
    for device in devices:
        known_devices.add(device["name"])
        try:
            if device["vendor"] == "nokia":
                interface_ip_map = get_nokia_interfaces_ip_map(device)
            elif device["vendor"] == "arista":
                interface_ip_map = get_arista_interfaces_ip_map(device)
            else:
                interface_ip_map = {}
        except Exception as e:
            print(f"[warn] no se pudieron obtener IPs de interfaces para {device['name']}: {e}")
            interface_ip_map = {}


        topology.append(
            build_device_topology_json(device, edges_map, node_ip, interface_ip_map)
        )

    all_nodes = set()

    for a, b in edges_map.keys():
        all_nodes.add(a)
        all_nodes.add(b)

    unknown_hosts = sorted(all_nodes - known_devices)

    for host in unknown_hosts:
        NODE_IP[host] = get_host_ip(host)
        topology.append(
            build_host_topology_json(host, edges_map)
        )

    return topology


def write_topology_json(devices, edges_map, node_ip, out="topology.json"):
    topology = build_topology_json(devices, edges_map, node_ip)

    with open(out, "w", encoding="utf-8") as f:
        json.dump(topology, f, indent=2, ensure_ascii=False)

    return topology


from deepdiff import DeepDiff
from pprint import pprint

INTERFACE_STATUS_EVENTS = {}
LAST_STATE = None
LAST_KNOWN_EDGES = {}

def refresh():
    global LAST_STATE, LAST_KNOWN_EDGES

    edges_total = {}
    nodes = set()
    NODE_STATUS = {}

    for device in DEVICES:
        NODE_STATUS[device["name"]] = check_device_status(device)


    for device in DEVICES:
        nodes.add(device["name"])
        try:
            edges_total.update(collect_edges(device))
        except subprocess.CalledProcessError as e:
            print(f"[warn] error consultando {device['name']} ({device['vendor']}): {e}")
        except Exception as e:
            print(f"[warn] error inesperado en {device['name']} ({device['vendor']}): {e}")

    for (a, b) in edges_total.keys():
        nodes.add(a)
        nodes.add(b)

    edges_with_status = {}


    for key, edge_data in edges_total.items():
        a, b = key
        edge_copy = edge_data.copy()


        left_status = get_current_interface_status( a, edge_data["left_interface"] )
        right_status = get_current_interface_status( b, edge_data["right_interface"] )


        if left_status == "down" or right_status == "down":
            edge_copy["status"] = "down"
        else:
            edge_copy["status"] = "up"

        edges_with_status[key] = edge_copy
        LAST_KNOWN_EDGES[key] = edge_copy

    for key, edge_data in LAST_KNOWN_EDGES.items():
        if key not in edges_with_status:
            edge_copy = edge_data.copy()
            edge_copy["status"] = "down"
            edges_with_status[key] = edge_copy
            nodes.add(key[0])
            nodes.add(key[1])


    current_state = {
    "nodes": sorted(nodes),
    "edges": edges_with_status
    }

    INTERFACE_IPS = {}

    for device in DEVICES:
        try:
            if device["vendor"] == "nokia":
                INTERFACE_IPS[device["name"]] = get_nokia_interfaces_ip_map(device)
            elif device["vendor"] == "arista":
                INTERFACE_IPS[device["name"]] = get_arista_interfaces_ip_map(device)
        except Exception as e:
            print(f"[warn] no se pudieron obtener IPs para draw.io de {device['name']}: {e}")
            INTERFACE_IPS[device["name"]] = {}

    if LAST_STATE is None:
        LAST_STATE = current_state
        known_devices = {d["name"] for d in DEVICES}
        for n in nodes:
            if n not in known_devices and n not in NODE_IP:
                NODE_IP[n] = get_host_ip(n)
        write_drawio(nodes, edges_with_status, NODE_STATUS, INTERFACE_IPS)
        topology = write_topology_json(DEVICES, edges_with_status, NODE_IP)
        send_topology_to_kafka(topology)
        return True


    diff = DeepDiff(LAST_STATE, current_state, ignore_order=True)

    if not diff:
        return False


    LAST_STATE = current_state
    known_devices = {d["name"] for d in DEVICES}
    for n in nodes:
        if n not in known_devices and n not in NODE_IP:
            NODE_IP[n] = get_host_ip(n)
    write_drawio(nodes, edges_with_status, NODE_STATUS, INTERFACE_IPS)
    topology = write_topology_json(DEVICES, edges_with_status, NODE_IP)
    send_topology_to_kafka(topology)

    return True


refresh_lock = threading.Lock()
last_refresh = 0
refresh_interval = 1.0


def handle_stream(device):
    global last_refresh

    path = PATHS[device["vendor"]]["if_state_stream"]

    cmd = [
        "gnmic", "-a", device["addr"],
        "-u", device["user"], "-p", device["password"],
    ]

    if device["vendor"] == "arista":
        cmd.append("--insecure")
    else:
        cmd.append("--skip-verify")

    cmd += [
        "subscribe",
        "--stream-mode", "on-change",
        "--path", path,
        "--format", "flat",
    ]


    p = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1
    )

    while True:
        line = p.stdout.readline()
        if not line:
            continue

        line = line.strip()
        if not line:
            continue


        m = re.search(r"interface\[name=([^\]]+)\].*oper-(?:state|status):\s*(\w+)", line, re.IGNORECASE)

        if m:
            intf_name = m.group(1)
            intf_status = m.group(2).lower()

            if intf_status in ("down", "lower-layer-down"):
                INTERFACE_STATUS_EVENTS[(device["name"], intf_name)] = "down"
            elif intf_status == "up":
                INTERFACE_STATUS_EVENTS[(device["name"], intf_name)] = "up"


        if "oper-status" in line.lower() or "oper-state" in line.lower():
            now = time.time()

            with refresh_lock:
                if now - last_refresh >= refresh_interval:
                    refresh()
                    last_refresh = now


def stream():
    try:
        load_node_ips_from_gnmi()
    except Exception as e:
        print(f"[warn] no se pudieron actualizar IPs de mgmt por gNMI: {e}")

    refresh()

    threads = []

    for device in DEVICES:
        t = threading.Thread(target=handle_stream, args=(device,), daemon=True)
        t.start()
        threads.append(t)


    while True:
        time.sleep(1)


if __name__ == "__main__":
    try:
        print("Devices cargados del ansible-inventory:")
        for d in DEVICES:
            print(f"  - {d['name']} ({d['vendor']}) - {d['addr']}")

        stream()

    except KeyboardInterrupt:
        print("\nFin")

