package langya.hbase.observer.util;

import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import langya.elasticsearch.util.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

/**
 * Created by Administrator on 2019/9/25.
 */
public class HbaseDataSyncEsObserver extends BaseRegionObserver {
    private static final Log LOGGER = LogFactory.getLog(HbaseDataSyncEsObserver.class);

    private String indexName;
    private String indexType;

    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        super.start(e);
        initIndexConfiguraction(e);
    }

    private void initIndexConfiguraction(CoprocessorEnvironment e){
        loadEsClientInfo(e);
    }

    private void loadEsClientInfo(CoprocessorEnvironment e){

        //集群名称
        ElasticsearchPoolUtil.CLUSTER_NAME = e.getConfiguration().get("cluster.name","");
        this.setIndexName(e.getConfiguration().get("indexName",""));
        this.setIndexType(e.getConfiguration().get("indexType",""));

        String esClientInfo = e.getConfiguration().get("esClientInfo","192.168.1.1:9300-192.168.1.2:9300");
        String[] esClientInfoList = esClientInfo.split("-");
        for(String esClientInfoTemp : esClientInfoList){
            ElasticsearchPoolUtil.esHostTcpList.add(esClientInfoTemp);
        }

    }

    @Override
    public void stop(CoprocessorEnvironment e) throws IOException {
        super.stop(e);
        ElasticsearchBulkOperator.shutdownScheduEx();
        ElasticsearchPoolUtil.destoryAllConnection();
    }

    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        super.postPut(e, put, edit, durability);
        String rowKey = new String(put.getRow());
        NavigableMap<byte[],List<Cell>> familys = put.getFamilyCellMap();
        Map<String,Object> result = new HashedMap();
        for(Map.Entry<byte[],List<Cell>> entry : familys.entrySet()){
            for(Cell cell : entry.getValue()){
                String key = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                result.put(key,value);
            }
        }

        try{

            TransportClient transportClient = ElasticsearchPoolUtil.getClient();
            UpdateRequestBuilder updateRequestBuilder = transportClient.prepareUpdate(this.indexName,this.indexType,rowKey);
            updateRequestBuilder.setDoc(result);
            updateRequestBuilder.setDocAsUpsert(true);
            ElasticsearchPoolUtil.returnClient(transportClient);
            ElasticsearchBulkOperator.addUpdateBuilderToBulk(updateRequestBuilder);
        }catch (Exception e1){
            LOGGER.error(e1);
        }

    }

    @Override
    public void postDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete, WALEdit edit, Durability durability) throws IOException {
        super.postDelete(e, delete, edit, durability);
        String rowKey = new String(delete.getRow());
        try{
            TransportClient transportClient = ElasticsearchPoolUtil.getClient();
            DeleteRequestBuilder deleteRequestBuilder = transportClient.prepareDelete(this.indexName,this.indexType,rowKey);
            ElasticsearchPoolUtil.returnClient(transportClient);
            ElasticsearchBulkOperator.addDeleteBuilderToBulk(deleteRequestBuilder);
        }catch (Exception e1){
            LOGGER.error(e1);
        }
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public void setIndexType(String indexType) {
        this.indexType = indexType;
    }

    public String getIndexName() {
        return indexName;
    }

    public String getIndexType() {
        return indexType;
    }
}
