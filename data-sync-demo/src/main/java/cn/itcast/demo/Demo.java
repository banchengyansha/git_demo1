package cn.itcast.demo;

import cn.itcast.pojo.Book;
import cn.itcast.util.BinlogValue;
import cn.itcast.util.CanalDataParser;
import cn.itcast.util.DateUtils;
import cn.itcast.util.InnerBinlogEntry;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.Time;
import java.text.ParseException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class Demo {

    private static Logger logger = LoggerFactory.getLogger(Demo.class);

    public static void main(String[] args) throws Exception {

        String host = "192.168.142.154";
        String destination = "example";
        Integer port = 11111;

        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress(host, port), destination, "", "");
        canalConnector.connect();

        canalConnector.subscribe();

        logger.info("连接canal server , host: " + host+" , port : " + port + ", destination : " + destination);

        while (true){
            Message message = canalConnector.getWithoutAck(5 * 1024);
            long messageId = message.getId();
            int size = message.getEntries().size();
            if(messageId == -1 || size == 0){
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                canalConnector.ack(messageId);
            }else{
                logger.info("binLog 日志开始解析 ...");

                List<InnerBinlogEntry> innerBinlogEntries = CanalDataParser.convertToInnerBinlogEntry(message);

                syncDataToSolr(innerBinlogEntries);

            }
        }
    }

    /**
     * 将 innerBinlogEntries 数据进行解析, 同步到solr 索引库
     * @param innerBinlogEntries
     */
    private static void syncDataToSolr(List<InnerBinlogEntry> innerBinlogEntries) throws Exception {

        SolrServer solrServer = new HttpSolrServer("http://192.168.142.152:8080/solr");

        if(innerBinlogEntries != null){
            for (InnerBinlogEntry innerBinlogEntry : innerBinlogEntries) {
                CanalEntry.EventType eventType = innerBinlogEntry.getEventType();//INSERT , UPDATE , DELETE
                System.out.println("=======> " + eventType);

                List<Map<String, BinlogValue>> list = innerBinlogEntry.getRows();

                //如果是增加, 或者是修改操作, 需要将数据添加/更新到solr索引库
                if(eventType == CanalEntry.EventType.INSERT || eventType == CanalEntry.EventType.UPDATE){
                    if(list != null){
                        for (Map<String, BinlogValue> map : list) {

                            Book book = new Book();

                            BinlogValue id = map.get("id");
                            BinlogValue name = map.get("name");
                            BinlogValue publishtime = map.get("publishtime");
                            BinlogValue author = map.get("author");
                            BinlogValue price = map.get("price");
                            BinlogValue publishgroup = map.get("publishgroup");

                            book.setId(Integer.parseInt(id.getValue()));
                            book.setPrice(Double.parseDouble(price.getValue()));
                            book.setPublishgroup(publishgroup.getValue());
                            book.setPublishtime(DateUtils.parseDate(publishtime.getValue()));
                            book.setAuthor(author.getValue());
                            book.setName(name.getValue());

                            logger.info("往solr索引库中同步数据,  同步数据的ID是 : " + id.getValue());

                            solrServer.addBean(book);
                            solrServer.commit();
                        }
                    }

                }else if(eventType == CanalEntry.EventType.DELETE){//如果执行的是删除操作, 则需要删除solr的索引库
                    if(list != null){
                        for (Map<String, BinlogValue> valueMap : list) {
                            BinlogValue id = valueMap.get("id");
                            String idValue = id.getValue();

                            logger.info("从solr索引库中删除数据,  删除数据的ID是 : " + id.getValue());

                            solrServer.deleteById(idValue);
                            solrServer.commit();
                        }
                    }
                }
            }
        }

    }

}
