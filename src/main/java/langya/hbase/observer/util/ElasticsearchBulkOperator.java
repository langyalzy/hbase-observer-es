package langya.hbase.observer.util;

import com.langya.elasticsearch.util.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by Administrator on 2019/9/25.
 */
public class ElasticsearchBulkOperator {
    private static final Log LOGGER = LogFactory.getLog(ElasticsearchBulkOperator.class);
    private static final Integer BULK_TIME_OUT = 5;//批次请求超时时间，单位：分钟

    private static final Integer CORE_POOL_SIZE = 3;//线程池大小

    private static final Integer PUT_INITIAL_DELAY = 10;//单位：秒
    private static final Integer PUT_PREIOD = 10;

    private static Integer idleIimeAccumulation = 0;//协处理器空闲时间积累，达到半小时就会断掉所有的连接，将所有的连接释放，减轻Elasticsearch的和HBase的压力

    private static final Integer SERVICE_MAXIMUM_IDLE_TIME = 1800;//服务最大空闲时间，单位：秒，默认半小时

    private static final Integer DESTOR_CONNECTION_INITIAL_DELAY = 10;//单位：秒

    private static final Integer DESTOR_CONNECTION_PERIOD = 3;//单位：秒，销毁多余连接检查间隔

    public static int MAX_BULK_COUNT = 10000;//最大批次提交，单位：条，用来限制不断写入的时候请求队列大小（通过安装协处理器的时候传递参数可以改变此默认值）

    public static int MIN_BULK_COUNT = 0;//最小批次提交大小，单位：条，用在周期性提交请求（周期性任务执行时，只要数据量 > 0条，就会提交），为了避免长时间不进数据，又没有达到最大请求队列限制，导致数据延迟问题（通过安装协处理器传递参数可以改变此默认值）

    private static BulkRequestBuilder bulkRequestBuilder = null;

    private static final Lock commitLock = new ReentrantLock();

    private static ScheduledExecutorService scheduledExecutorService;

    static {
        bulkRequestBuilder();
        scheduledExecutorService = Executors.newScheduledThreadPool(CORE_POOL_SIZE);
        final Runnable beeper = () -> run();
        final Runnable destoryRedundantConnection = () -> destoryRedundantConnections();
        scheduledExecutorService.scheduleAtFixedRate(beeper, PUT_INITIAL_DELAY, PUT_PREIOD, TimeUnit.SECONDS);//定时刷新数据到Elasticsearch中
        scheduledExecutorService.scheduleAtFixedRate(destoryRedundantConnection, DESTOR_CONNECTION_INITIAL_DELAY, DESTOR_CONNECTION_PERIOD, TimeUnit.SECONDS);//定时清除多余Elasticsearch连接

    }

    private static void serviceIdleCheck() {
        synchronized (bulkRequestBuilder) {
            System.out.println("写入队列当前大小：" + bulkRequestBuilder.numberOfActions() + "   服务空闲时间累积:" + idleIimeAccumulation + "秒");
            if (bulkRequestBuilder.numberOfActions() == 0) {
                if (idleIimeAccumulation < SERVICE_MAXIMUM_IDLE_TIME) {
                    idleIimeAccumulation += DESTOR_CONNECTION_PERIOD;
                } else {
                    ElasticsearchPoolUtil.destoryAllConnection();
                    idleIimeAccumulation = 0;
                }
            } else {
                idleIimeAccumulation = 0;
            }
        }
    }

    private static void bulkRequestBuilder() {
        try {

            TransportClient client = ElasticsearchPoolUtil.getClient();
            bulkRequestBuilder = client.prepareBulk();
            ElasticsearchPoolUtil.returnClient(client);

        } catch (Exception e) {
            LOGGER.error(e);
        }
        bulkRequestBuilder.setTimeout(TimeValue.timeValueMinutes(BULK_TIME_OUT));
    }

    private static void run() {
        try {
            commitLock.lock();
            bulkRequest(MIN_BULK_COUNT);
        } catch (Exception ex) {
            LOGGER.error("Time Bulk index error:" + ex.getMessage());
        } finally {
            commitLock.unlock();
        }

    }


    private static void destoryRedundantConnections() {
        ElasticsearchPoolUtil.pilotConnection();
        serviceIdleCheck();
    }

    public static void shutdownScheduEx() {
        if (null != scheduledExecutorService && !scheduledExecutorService.isShutdown()) {
            scheduledExecutorService.shutdown();
        }
    }

    private static void bulkRequest(int threshold) {
        if (bulkRequestBuilder.numberOfActions() > threshold) {
            try {
                BulkResponse bulkItemResponses = bulkRequestBuilder.execute().actionGet();
                if (!bulkItemResponses.hasFailures()) {
                    TransportClient client = ElasticsearchPoolUtil.getClient();
                    bulkRequestBuilder = client.prepareBulk();
                    LOGGER.info("批次提交");
                    ElasticsearchPoolUtil.returnClient(client);
                }
            } catch (Exception ex) {
                try {
                    TransportClient client = ElasticsearchPoolUtil.getClient();
                    List<DocWriteRequest> tempRequests = bulkRequestBuilder.request().requests();
                    bulkRequestBuilder = client.prepareBulk();
                    bulkRequestBuilder.request().add(tempRequests);
                    ElasticsearchPoolUtil.returnClient(client);
                } catch (Exception es) {
                    LOGGER.error(es);
                }
                LOGGER.error(ex);
            }
        }
    }

    public static void addUpdateBuilderToBulk(UpdateRequestBuilder builder) {
        commitLock.lock();
        try {
            bulkRequestBuilder.add(builder);
            bulkRequest(MAX_BULK_COUNT);
        } catch (Exception e) {
            LOGGER.error(e);
        } finally {
            commitLock.unlock();
        }
    }

    public static void addDeleteBuilderToBulk(DeleteRequestBuilder builder) {
        commitLock.lock();
        try {
            bulkRequestBuilder.add(builder);
            bulkRequest(MAX_BULK_COUNT);
        } catch (Exception e) {
            LOGGER.error(e);
        } finally {
            commitLock.unlock();
        }
    }

}
