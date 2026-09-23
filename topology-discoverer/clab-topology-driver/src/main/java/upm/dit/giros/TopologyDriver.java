package upm.dit.giros;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Map.Entry;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import java.util.*;

import org.opendaylight.mdsal.binding.dom.codec.api.BindingNormalizedNodeSerializer;
import org.opendaylight.mdsal.binding.dom.codec.impl.BindingCodecContext;
import org.opendaylight.mdsal.binding.runtime.spi.BindingRuntimeHelpers;
import org.opendaylight.mdsal.binding.spec.reflect.BindingReflections;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;
import org.opendaylight.yangtools.yang.data.api.YangInstanceIdentifier;
import org.opendaylight.yangtools.yang.data.api.schema.MapEntryNode;
import org.opendaylight.yangtools.yang.data.api.schema.NormalizedNode;
import org.opendaylight.yangtools.yang.data.api.schema.stream.NormalizedNodeStreamWriter;
import org.opendaylight.yangtools.yang.data.api.schema.stream.NormalizedNodeWriter;
import org.opendaylight.yangtools.yang.data.codec.gson.JSONCodecFactory;
import org.opendaylight.yangtools.yang.data.codec.gson.JSONCodecFactorySupplier;
import org.opendaylight.yangtools.yang.data.codec.gson.JSONNormalizedNodeStreamWriter;
import org.opendaylight.yangtools.yang.data.codec.gson.JsonWriterFactory;
import org.opendaylight.yangtools.yang.model.api.EffectiveModelContext;
import org.opendaylight.yangtools.yang.model.api.SchemaPath;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;

import org.opendaylight.yangtools.yang.data.codec.xml.XMLStreamNormalizedNodeStreamWriter;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import org.opendaylight.yang.gen.v1.urn.ietf.params.xml.ns.yang.ietf.network.rev180226.Networks;
import org.opendaylight.yang.gen.v1.urn.ietf.params.xml.ns.yang.ietf.network.rev180226.NetworkId;
import org.opendaylight.yang.gen.v1.urn.ietf.params.xml.ns.yang.ietf.network.rev180226.NodeId;
import org.opendaylight.yang.gen.v1.urn.ietf.params.xml.ns.yang.ietf.network.rev180226.NetworksBuilder;
import org.opendaylight.yang.gen.v1.urn.ietf.params.xml.ns.yang.ietf.network.rev180226.networks.Network;
import org.opendaylight.yang.gen.v1.urn.ietf.params.xml.ns.yang.ietf.network.rev180226.networks.NetworkBuilder;
import org.opendaylight.yang.gen.v1.urn.ietf.params.xml.ns.yang.ietf.network.rev180226.networks.NetworkKey;
import org.opendaylight.yang.gen.v1.urn.ietf.params.xml.ns.yang.ietf.network.rev180226.networks.network.Node;
import org.opendaylight.yang.gen.v1.urn.ietf.params.xml.ns.yang.ietf.network.rev180226.networks.network.NodeBuilder;
import org.opendaylight.yang.gen.v1.urn.ietf.params.xml.ns.yang.ietf.network.rev180226.networks.network.NodeKey;

import org.opendaylight.yang.gen.v1.urn.ietf.params.xml.ns.yang.ietf.network.topology.rev180226.Network1Builder;
import org.opendaylight.yang.gen.v1.urn.ietf.params.xml.ns.yang.ietf.network.topology.rev180226.Network1;
import org.opendaylight.yang.gen.v1.urn.ietf.params.xml.ns.yang.ietf.network.topology.rev180226.Node1Builder;
import org.opendaylight.yang.gen.v1.urn.ietf.params.xml.ns.yang.ietf.network.topology.rev180226.Node1;
import org.opendaylight.yang.gen.v1.urn.ietf.params.xml.ns.yang.ietf.network.topology.rev180226.LinkId;
import org.opendaylight.yang.gen.v1.urn.ietf.params.xml.ns.yang.ietf.network.topology.rev180226.TpId;
import org.opendaylight.yang.gen.v1.urn.ietf.params.xml.ns.yang.ietf.network.topology.rev180226.networks.network.Link;
import org.opendaylight.yang.gen.v1.urn.ietf.params.xml.ns.yang.ietf.network.topology.rev180226.networks.network.LinkBuilder;
import org.opendaylight.yang.gen.v1.urn.ietf.params.xml.ns.yang.ietf.network.topology.rev180226.networks.network.LinkKey;
import org.opendaylight.yang.gen.v1.urn.ietf.params.xml.ns.yang.ietf.network.topology.rev180226.networks.network.link.Source;
import org.opendaylight.yang.gen.v1.urn.ietf.params.xml.ns.yang.ietf.network.topology.rev180226.networks.network.link.SourceBuilder;
import org.opendaylight.yang.gen.v1.urn.ietf.params.xml.ns.yang.ietf.network.topology.rev180226.networks.network.link.Destination;
import org.opendaylight.yang.gen.v1.urn.ietf.params.xml.ns.yang.ietf.network.topology.rev180226.networks.network.link.DestinationBuilder;
import org.opendaylight.yang.gen.v1.urn.ietf.params.xml.ns.yang.ietf.network.topology.rev180226.networks.network.node.TerminationPoint;
import org.opendaylight.yang.gen.v1.urn.ietf.params.xml.ns.yang.ietf.network.topology.rev180226.networks.network.node.TerminationPointBuilder;
import org.opendaylight.yang.gen.v1.urn.ietf.params.xml.ns.yang.ietf.network.topology.rev180226.networks.network.node.TerminationPointKey;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import com.google.gson.JsonStreamParser;
import com.google.gson.stream.JsonWriter;
import com.google.gson.JsonParseException;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.lang.String;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Java application based on the YANG Tools library for parsing data from network topology descriptor based on the 
 * ContainerLab simulation testbed and mapping it to YANG-compliant data according to the ietf-network and 
 * ietf-network-topology YANG data models (RFC 8345: https://datatracker.ietf.org/doc/html/rfc8345).
 */
public class TopologyDriver {

    public static void main(String[] args) throws Exception {

        // GET EXECUTION ENVIRONMENT
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // KAFKA CONSUMER
        KafkaSource<String> consumer = KafkaSource.<String>builder()
                .setTopics(args[1])
                .setGroupId("topology-driver-source-group")
                .setBootstrapServers(args[0])
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer((DeserializationSchema<String>) new SimpleStringSchema())
                .build();

        // KAFKA PRODUCERS
        KafkaSink<String> producer_json = KafkaSink.<String>builder()
                .setBootstrapServers(args[0])
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(args[2])
                        .setValueSerializationSchema((SerializationSchema<String>) new SimpleStringSchema())
                        .build())
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        KafkaSink<String> producer_xml = KafkaSink.<String>builder()
                .setBootstrapServers(args[0])
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(args[3])
                        .setValueSerializationSchema((SerializationSchema<String>) new SimpleStringSchema())
                        .build())
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
        
        DataStream<String> stringInputStream = env.fromSource(consumer, WatermarkStrategy.noWatermarks(),
                "Kafka Source");

        DataStream<String> json_ietf = stringInputStream.map(new MapFunction<String, String>() {
            @Override
            public String map(String json) throws Exception {
                try {
                    json = topology_driver_json(json);
                } catch (JsonParseException | NullPointerException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                    json = "";
                }
                return json;
            }
        }).filter(new FilterFunction<String>() {
            // make sure only valid json values are written into the topic
            @Override
            public boolean filter(String value) throws Exception {
                // if empty do not return
                return !value.equals("");
            }
        });

        DataStream<String> xml_ietf = stringInputStream.map(new MapFunction<String, String>() {
            @Override
            public String map(String json) throws Exception {
                try {
                    json = topology_driver_xml(json);
                } catch (JsonParseException | NullPointerException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                    json = "";
                }
                return json;
            }
        }).filter(new FilterFunction<String>() {
            // make sure only valid json values are written into the topic
            @Override
            public boolean filter(String value) throws Exception {
                // if empty do not return
                return !value.equals("");
            }
        });

        json_ietf.sinkTo(producer_json);
        xml_ietf.sinkTo(producer_xml);

        // Execute program
        env.execute("Topology driver");

    }

    public static String topology_driver_json(String jsonString) throws Exception {

        String network_id = new String();
        ArrayList<String> nodes_id = new ArrayList<String>();
        //ArrayList<String> nodes_kind = new ArrayList<String>();
        
        Map<String, ArrayList<String>> node_termination_points = new HashMap<String, ArrayList<String>>();
        Map<String, ArrayList<String>> node_links = new HashMap<String, ArrayList<String>>();

        NetworksBuilder networks_builder = new NetworksBuilder();
        Map<NetworkKey, Network> map_networks = new HashMap<NetworkKey, Network>();
        NetworkBuilder network_builder = new NetworkBuilder();
        Network1Builder network_aug_builder = new Network1Builder();
        Map<NodeKey, Node> map_nodes = new HashMap<NodeKey, Node>();
        NodeBuilder node_builder = new NodeBuilder();
        Node1Builder node_aug_builder = new Node1Builder();
        Map<LinkKey, Link> map_links = new HashMap<LinkKey, Link>();
        LinkBuilder link_builder = new LinkBuilder();
        SourceBuilder link_source_builder = new SourceBuilder();
        DestinationBuilder link_destination_builder = new DestinationBuilder();
        TerminationPointBuilder tp_builder = new TerminationPointBuilder();
        
        //JSON Object to parse JSON file
        //JsonObject topo = JsonParser.parseString(jsonString).getAsJsonObject();

        // se coloca JsonElement porque el json empieza con [] y no con { JsonElement puede ser array 
        //JsonElement topo = JsonParser.parseString(jsonString);
        JsonArray devices = JsonParser.parseString(jsonString).getAsJsonArray();
        network_id = "python-topology";
        network_builder.setNetworkId(NetworkId.getDefaultInstance(network_id));

        //network_id = topo.get("name").getAsString();
        //network_builder.setNetworkId(NetworkId.getDefaultInstance(network_id));

        for (int i = 0; i < devices.size(); i++) {
            JsonObject device = devices.get(i).getAsJsonObject();
            nodes_id.add(device.get("container").getAsString());
        }


        Set<String> links_seen = new HashSet<String>();  //test delete duplicate
        for (int i = 0; i < devices.size(); i++) {
            JsonObject device = devices.get(i).getAsJsonObject();
            String localNode = device.get("container").getAsString();
            ArrayList<String> n_links = new ArrayList<String>();
            ArrayList<String> n_tp = new ArrayList<String>();
            Map<TerminationPointKey, TerminationPoint> map_tp = new HashMap<TerminationPointKey, TerminationPoint>();
            JsonArray interfaces = device.getAsJsonArray("interfaces");
            for (int j = 0; j < interfaces.size(); j++) {
                JsonObject iface = interfaces.get(j).getAsJsonObject();
                String localInterface = iface.get("name").getAsString();
                //skip mng interface to avoid confusion in the topology
                if (device.has("mgmt_interface") && localInterface.equals(device.get("mgmt_interface").getAsString())) {
                    continue;
                }
                tp_builder.setTpId(TpId.getDefaultInstance(localInterface));
                TerminationPoint tp = tp_builder.build();
                map_tp.put(tp.key(), tp);
                n_tp.add(localInterface);
                if (iface.has("adjacent-node") && iface.has("adjacent-interface")) {
                    if (iface.has("status") && iface.get("status").getAsString().equalsIgnoreCase("down")) {
                        continue;
                    }
                    String remoteNode = iface.get("adjacent-node").getAsString();
                    String remoteInterface = iface.get("adjacent-interface").getAsString();
                    // try example r1 <-> r2 are the same that r2 <-> r1
                    String endpointA = localNode + ":" + localInterface;
                    String endpointB = remoteNode + ":" + remoteInterface;
                    String linkKey;
                    if (endpointA.compareTo(endpointB) < 0) {
                        linkKey = endpointA + "<->" + endpointB;
                    } else {
                        linkKey = endpointB + "<->" + endpointA;
                    }
                    if (links_seen.contains(linkKey)) {
                        n_links.add(localNode + ":" + localInterface + "<->" + remoteNode + ":" + remoteInterface);
                        continue;
                    }
                    links_seen.add(linkKey);
                    link_source_builder.setSourceNode(NodeId.getDefaultInstance(localNode));
                    link_source_builder.setSourceTp((Object)localInterface);
                    link_destination_builder.setDestNode(NodeId.getDefaultInstance(remoteNode));
                    link_destination_builder.setDestTp((Object)remoteInterface);
                    link_builder.setLinkId(LinkId.getDefaultInstance(
                        localNode + "-" + localInterface + "-" + remoteNode + "-" + remoteInterface
                    ));

                    Source link_source = link_source_builder.build();
                    Destination link_destination = link_destination_builder.build();
                    link_builder.setSource(link_source);
                    link_builder.setDestination(link_destination);
                    Link link_instance = link_builder.build();
                    map_links.put(link_instance.key(), link_instance);
                    n_links.add(localNode + ":" + localInterface + "<->" + remoteNode + ":" + remoteInterface);
                }

            }

            node_aug_builder.setTerminationPoint(map_tp);
            Node1 node_aug = node_aug_builder.build();

            node_builder.setNodeId(NodeId.getDefaultInstance(localNode));
            node_builder.addAugmentation(node_aug);

            Node node = node_builder.build();
            map_nodes.put(node.key(), node);

            node_termination_points.put(localNode, n_tp);
            node_links.put(localNode, n_links);
        }

        network_aug_builder.setLink(map_links);
        Network1 network_aug = network_aug_builder.build();
        network_builder.addAugmentation(network_aug);
        network_builder.setNode(map_nodes);
        Network network = network_builder.build();
        map_networks.put(network.key(), network);
        networks_builder.setNetwork(map_networks);
        

        //System.out.println("\nNETWORKS BUILDER: " + networks_builder.build().toString());

        final Networks networks = networks_builder.build();

        InstanceIdentifier<Networks> iid = InstanceIdentifier.create(Networks.class);

        //System.out.println("\nNetworks InstanceIdentifier (iid): " + iid);

        JsonObject network_topology_json = new JsonObject();
    
        try {
            BindingNormalizedNodeSerializer codec = new BindingCodecContext(BindingRuntimeHelpers.createRuntimeContext());
            Entry<YangInstanceIdentifier, NormalizedNode<?, ?>> normalized = codec.toNormalizedNode(iid, networks);
            network_topology_json = doConvert(schemaContext.getPath(), normalized.getValue());
        } catch (Exception ex) {
                ex.printStackTrace();
                StringWriter errors = new StringWriter();
                ex.printStackTrace(new PrintWriter(errors));
                //LOG.error(errors.toString());
        }

        //System.out.println("\nJSON Network Topology: \n" + network_topology_json.toString());

        Gson json_format = new GsonBuilder().setPrettyPrinting().create();

        System.out.println("\nJSON Network Topology: \n" + json_format.toJson(network_topology_json));

        System.out.println("\nNETWORK ID: " + network_id);

        System.out.println("\nNODES IDs: " + nodes_id.toString());
        
        System.out.println("\nTERMINATION POINTS: ");

        Iterator it_node_tps = node_termination_points.keySet().iterator();
        while (it_node_tps.hasNext()){
            String key = (String) it_node_tps.next();
            System.out.println("Node: " + key + " -> Termination Points: " + node_termination_points.get(key));
        }

        System.out.println("\nLINKS: ");

        Iterator it_node_links = node_links.keySet().iterator();
        while (it_node_links.hasNext()){
            String key = (String) it_node_links.next();
            System.out.println("Node: " + key + " -> Links: " + node_links.get(key));
        }
        
        return network_topology_json.toString();
    }

    public static String topology_driver_xml(String jsonString) throws Exception {

        String network_id = new String();
        ArrayList<String> nodes_id = new ArrayList<String>();
        //ArrayList<String> nodes_kind = new ArrayList<String>();
        
        Map<String, ArrayList<String>> node_termination_points = new HashMap<String, ArrayList<String>>();
        Map<String, ArrayList<String>> node_links = new HashMap<String, ArrayList<String>>();

        NetworksBuilder networks_builder = new NetworksBuilder();
        Map<NetworkKey, Network> map_networks = new HashMap<NetworkKey, Network>();
        NetworkBuilder network_builder = new NetworkBuilder();
        Network1Builder network_aug_builder = new Network1Builder();
        Map<NodeKey, Node> map_nodes = new HashMap<NodeKey, Node>();
        NodeBuilder node_builder = new NodeBuilder();
        Node1Builder node_aug_builder = new Node1Builder();
        Map<LinkKey, Link> map_links = new HashMap<LinkKey, Link>();
        LinkBuilder link_builder = new LinkBuilder();
        SourceBuilder link_source_builder = new SourceBuilder();
        DestinationBuilder link_destination_builder = new DestinationBuilder();
        TerminationPointBuilder tp_builder = new TerminationPointBuilder();
          
        JsonArray devices = JsonParser.parseString(jsonString).getAsJsonArray();

        network_id = "python-topology";
        network_builder.setNetworkId(NetworkId.getDefaultInstance(network_id));
        Set<String> links_seen = new HashSet<String>();
        for (int i = 0; i < devices.size(); i++) {
            JsonObject device = devices.get(i).getAsJsonObject();
            nodes_id.add(device.get("container").getAsString());
        }

        for (int i = 0; i < devices.size(); i++) {
            JsonObject device = devices.get(i).getAsJsonObject();
            String localNode = device.get("container").getAsString();
            ArrayList<String> n_links = new ArrayList<String>();
            ArrayList<String> n_tp = new ArrayList<String>();
            Map<TerminationPointKey, TerminationPoint> map_tp = new HashMap<TerminationPointKey, TerminationPoint>();
            JsonArray interfaces = device.getAsJsonArray("interfaces");
            for (int j = 0; j < interfaces.size(); j++) {
                JsonObject iface = interfaces.get(j).getAsJsonObject();
                String localInterface = iface.get("name").getAsString();
                if (device.has("mgmt_interface") && localInterface.equals(device.get("mgmt_interface").getAsString())) {
                    continue;
                }
                tp_builder.setTpId(TpId.getDefaultInstance(localInterface));
                TerminationPoint tp = tp_builder.build();
                map_tp.put(tp.key(), tp);
                n_tp.add(localInterface);
                if (iface.has("adjacent-node") && iface.has("adjacent-interface")) {
                    if (iface.has("status") && iface.get("status").getAsString().equalsIgnoreCase("down")) {
                        continue;
                    }                    
                    String remoteNode = iface.get("adjacent-node").getAsString();
                    String remoteInterface = iface.get("adjacent-interface").getAsString();
                    String endpointA = localNode + ":" + localInterface;
                    String endpointB = remoteNode + ":" + remoteInterface;
                    String linkKey;
                    if (endpointA.compareTo(endpointB) < 0) {
                        linkKey = endpointA + "<->" + endpointB;
                    } else {
                        linkKey = endpointB + "<->" + endpointA;
                    }
                    if (links_seen.contains(linkKey)) {
                        n_links.add(localNode + ":" + localInterface + "<->" + remoteNode + ":" + remoteInterface);
                        continue;
                    }
                    links_seen.add(linkKey);
                    link_source_builder.setSourceNode(NodeId.getDefaultInstance(localNode));
                    link_source_builder.setSourceTp((Object)localInterface);
                    link_destination_builder.setDestNode(NodeId.getDefaultInstance(remoteNode));
                    link_destination_builder.setDestTp((Object)remoteInterface);
                    link_builder.setLinkId(LinkId.getDefaultInstance(
                        localNode + "-" + localInterface + "-" + remoteNode + "-" + remoteInterface
                    ));

                    Source link_source = link_source_builder.build();
                    Destination link_destination = link_destination_builder.build();
                    link_builder.setSource(link_source);
                    link_builder.setDestination(link_destination);
                    Link link_instance = link_builder.build();
                    map_links.put(link_instance.key(), link_instance);
                    n_links.add(localNode + ":" + localInterface + "<->" + remoteNode + ":" + remoteInterface);
                }
            }

            node_aug_builder.setTerminationPoint(map_tp);
            Node1 node_aug = node_aug_builder.build();
            node_builder.setNodeId(NodeId.getDefaultInstance(localNode));
            node_builder.addAugmentation(node_aug);
            Node node = node_builder.build();
            map_nodes.put(node.key(), node);
            node_termination_points.put(localNode, n_tp);
            node_links.put(localNode, n_links);
        }


        network_aug_builder.setLink(map_links);
        Network1 network_aug = network_aug_builder.build();
        network_builder.addAugmentation(network_aug);
        network_builder.setNode(map_nodes);
        Network network = network_builder.build();
        map_networks.put(network.key(), network);
        networks_builder.setNetwork(map_networks);
        

        //System.out.println("\nNETWORKS BUILDER: " + networks_builder.build().toString());

        final Networks networks = networks_builder.build();

        InstanceIdentifier<Networks> iid = InstanceIdentifier.create(Networks.class);

        //System.out.println("\nNetworks InstanceIdentifier (iid): " + iid);

        String network_topology_xml = new String();
    
        try {
            BindingNormalizedNodeSerializer codec = new BindingCodecContext(BindingRuntimeHelpers.createRuntimeContext());
            Entry<YangInstanceIdentifier, NormalizedNode<?, ?>> normalized = codec.toNormalizedNode(iid, networks);
            network_topology_xml = doConvertXML(schemaContext.getPath(), normalized.getValue());
        } catch (Exception ex) {
                ex.printStackTrace();
                StringWriter errors = new StringWriter();
                ex.printStackTrace(new PrintWriter(errors));
                //LOG.error(errors.toString());
        }

        //System.out.println("\nJSON Network Topology: \n" + network_topology_json.toString());

        //System.out.println("\nXML Network Topology: \n" + network_topology_xml);

        InputSource src = new InputSource(new StringReader(network_topology_xml));
        Document document = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(src);
        TransformerFactory transformerFactory = TransformerFactory.newInstance();
        transformerFactory.setAttribute("indent-number", 2);
        Transformer transformer = TransformerFactory.newInstance().newTransformer();
        transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
        transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, true ? "yes" : "no");
        transformer.setOutputProperty(OutputKeys.INDENT, "yes");
        //transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
        Writer transformer_output = new StringWriter();
        transformer.transform(new DOMSource(document),new StreamResult(transformer_output));
        network_topology_xml = transformer_output.toString();

        System.out.println("\nXML Network Topology: \n" + network_topology_xml);

        System.out.println("\nNETWORK ID: " + network_id);

        System.out.println("\nNODES IDs: " + nodes_id.toString());
        
        System.out.println("\nTERMINATION POINTS: ");

        Iterator it_node_tps = node_termination_points.keySet().iterator();
        while (it_node_tps.hasNext()){
            String key = (String) it_node_tps.next();
            System.out.println("Node: " + key + " -> Termination Points: " + node_termination_points.get(key));
        }

        System.out.println("\nLINKS: ");

        Iterator it_node_links = node_links.keySet().iterator();
        while (it_node_links.hasNext()){
            String key = (String) it_node_links.next();
            System.out.println("Node: " + key + " -> Links: " + node_links.get(key));
        }
        
        return network_topology_xml;
    }

    // private static String getInterfaceId(String node_kind, String node_iface_id){
    //     String node_iface_name = new String();
    //     String node_iface_index = new String();
    //     switch (node_kind) {
    //         case "vr-cisco_csr1000v":
    //             node_iface_index = node_iface_id.replace("eth", "");
    //             node_iface_name = "GigabitEthernet" + String.valueOf(Integer.parseInt(node_iface_index) + 1);
    //             break;
    //         case "vr-cisco_xrv9k":
    //             node_iface_index = node_iface_id.replace("eth", "");
    //             node_iface_name = "GigabitEthernet0/0/0/" + String.valueOf(Integer.parseInt(node_iface_index) - 1);
    //             break;
    //         case "ceos":
    //             node_iface_index = node_iface_id.replace("eth", "");
    //             node_iface_name = "Ethernet" + String.valueOf(Integer.parseInt(node_iface_index));
    //             break;
    //         case "nokia_srlinux":
    //             node_iface_index = node_iface_id.replace("e", "");
    //             node_iface_name = "ethernet-" + node_iface_index.split("-")[0] + "/" + node_iface_index.split("-")[1];
    //             break;
    //         default:
    //             node_iface_name = node_iface_id;
    //     }
    //     return node_iface_name;
    // }
    // Schema context initialization
    // Code borrowed from:
    // https://github.com/opendaylight/jsonrpc/blob/1331a9f73db2fe308e3bbde00ff42359431dbc7f/
    // binding-adapter/src/main/java/org/opendaylight/jsonrpc/binding/EmbeddedRpcInvocationAdapter.java#L38
    private static final EffectiveModelContext schemaContext = BindingRuntimeHelpers
            .createEffectiveModel(BindingReflections.loadModuleInfos());

    // Code borrowed from:
    // https://git.opendaylight.org/gerrit/gitweb?p=jsonrpc.git;a=blob_plain;f=impl/src/
    // main/java/org/opendaylight/jsonrpc/impl/
    // JsonConverter.java;h=ea8069c67ece073e3d9febb694c4e15b01238c10;hb=3ea331d0e57712654d9ecbf2ae2a46cb0ce02d31
    private static final String JSON_IO_ERROR = "I/O problem in JSON codec";
    private static final String XML_IO_ERROR = "I/O problem in XML codec";
    //private static final Logger LOG = LogManager.getLogger(TopologyDriver.class);
    private static final JSONCodecFactorySupplier CODEC_SUPPLIER = JSONCodecFactorySupplier.RFC7951;
    private static final JsonParser PARSER = new JsonParser();

    /**
     * Performs the actual JSON data conversion.
     *
     * @param schemaPath - schema path for data
     * @param data       - Normalized Node
     * @return data converted as a JsonObject
     */
    private static JsonObject doConvert(SchemaPath schemaPath, NormalizedNode<?, ?> data) {
        try (StringWriter writer = new StringWriter();
                JsonWriter jsonWriter = JsonWriterFactory.createJsonWriter(writer)) {
            final JSONCodecFactory codecFactory = CODEC_SUPPLIER.getShared(schemaContext);
            final NormalizedNodeStreamWriter jsonStream = (data instanceof MapEntryNode)
                    ? JSONNormalizedNodeStreamWriter.createNestedWriter(codecFactory, schemaPath, null, jsonWriter)
                    : JSONNormalizedNodeStreamWriter.createExclusiveWriter(codecFactory, schemaPath, null, jsonWriter);
            try (NormalizedNodeWriter nodeWriter = NormalizedNodeWriter.forStreamWriter(jsonStream)) {
                nodeWriter.write(data);
                nodeWriter.flush();
            }
            return PARSER.parse(writer.toString()).getAsJsonObject();
        } catch (IOException e) {
            //LOG.error(JSON_IO_ERROR, e);
            return null;
        }
    }
    
    /**
     * Performs the actual XML data conversion.
     * @param schemaPath
     * @param data
     * @return
     * @throws XMLStreamException
     * @throws FactoryConfigurationError
     * @throws ParserConfigurationException
     * @throws SAXException
     */
    private static String doConvertXML(SchemaPath schemaPath, NormalizedNode<?, ?> data) throws XMLStreamException, FactoryConfigurationError, ParserConfigurationException, SAXException {
        try (StringWriter writer = new StringWriter()) {
            XMLStreamWriter xmlWriter = XMLOutputFactory.newInstance().createXMLStreamWriter(writer);
            final NormalizedNodeStreamWriter xmlStream = XMLStreamNormalizedNodeStreamWriter.create(xmlWriter,schemaContext, schemaPath);
            try (NormalizedNodeWriter nodeWriter = NormalizedNodeWriter.forStreamWriter(xmlStream)) {
                nodeWriter.write(data);
                nodeWriter.flush();
            }
            return writer.toString(); 
        } catch (IOException e) {
            //LOG.error(XML_IO_ERROR, e);
            return null;
        }
    }
}