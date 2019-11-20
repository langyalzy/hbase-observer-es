package langya.elasticsearch.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by Administrator on 2019/9/21.
 */
public class ElasticsearchUtil {
    private static final Log LOGGER = LogFactory.getLog(ElasticsearchUtil.class);

    private static String ID = "id";
    private static String INDEX = "index";
    private static String TYPE = "type";
    private static String DATAJSON = "dataJson";
    public static Integer MAX_CONNECT_SIZE=1;//默认最小连接数
    private static Integer MAX_RETRY_SIZE=600;//默认重试次数

    //静态的Connection队列
    private static LinkedList<TransportClient> clientQueue = null;

    public static String CLUSTER_NAME="langya-es-cluster";

    private static String ES_NODE_ONE="192.168.1.100";
    private static String ES_PORT_ONE="9300";
    public static List<String> esHostTcpList = new ArrayList<>();

    public synchronized static TransportClient getClient() throws Exception{

        if(clientQueue ==null ){
            clientQueue = new LinkedList<>();
            for(int i=0;i<MAX_CONNECT_SIZE;i++){
                clientQueue.push(clientPush());
            }
        }else if(clientQueue.size() == 0){
            clientQueue.push(clientPush());
        }
        return clientQueue.poll();

    }

    public static TransportAddress[] initTranSportAddress(){
        TransportAddress[] transportAddresses = new TransportAddress[esHostTcpList.size()];
        int offset = 0;
        for(int i=0;i<esHostTcpList.size();i++){
            String[] ipHost = esHostTcpList.get(i).split(":");
            try{
                transportAddresses[offset] = new TransportAddress(InetAddress.getByName(ipHost[0].trim()),Integer.valueOf(ipHost[1].trim()));
                offset++;

            }catch(Exception e){
                LOGGER.error("exec init transport address error:",e);
            }
        }
        return transportAddresses;
    }

    public static void pilotConnection(){
        synchronized (clientQueue){
            long startTime = System.currentTimeMillis();
            LOGGER.warn("正在啟動控制連接，目前連接數量為："+clientQueue.size());
            if(clientQueue.size()>MAX_CONNECT_SIZE){
                clientQueue.getLast().close();
                clientQueue.removeLast();
                LOGGER.warn("關閉連接耗時："+(System.currentTimeMillis()-startTime));
                pilotConnection();
            }else{
                return;
            }
        }
    }

    public static void destoryAllConnection(){
        LOGGER.warn("正在銷毀連接，目前連接數為"+clientQueue.size());
        synchronized (clientQueue){
            long startTime = System.currentTimeMillis();
            if(clientQueue.size()>0){
                clientQueue.getLast().close();
                clientQueue.removeLast();
                LOGGER.warn("關閉連接耗時："+(System.currentTimeMillis()-startTime));
                pilotConnection();
            }else{
                return;
            }
        }
    }

    private synchronized static TransportClient clientPush() throws Exception{

        TransportClient client = null;
        int upCount = 0;
        while(clientQueue.size()<MAX_CONNECT_SIZE && client == null && upCount < MAX_RETRY_SIZE){
            client = init();
            Thread.sleep(100);
            upCount++;
        }
        if(client == null){
            throw new Exception("Es client init failed wait for 60s");
        }
        return client;
    }

    public static TransportClient init()throws Exception{
        System.setProperty("es.set.netty.runtime.available.processors","true");
        Settings esSettings = Settings.builder().put("cluster.name",CLUSTER_NAME).put("client.transport.sniff",false).build();
        TransportClient client = new PreBuiltTransportClient(esSettings);
        client.addTransportAddresses(initTranSportAddress());
        return client;
    }

    public synchronized static void returnClient(TransportClient client){
        if(clientQueue ==null){
            clientQueue  = new LinkedList<>();
        }
        clientQueue.push(client);
    }

}
