package com.atguigu.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.atguigu.constants.GmallConstant;
import com.atguigu.utils.MyKafkaSender;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

public class CanalClient {

    public static void main(String[] args) throws InvalidProtocolBufferException {

        //1.获取Canal连接对象
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop102", 11111),
                "example",
                "",
                "");

        //2.抓取数据并解析
        while (true) {

            //连接
            canalConnector.connect();
            //指定消费的数据表
            canalConnector.subscribe("gmall200621.*");
            //抓取数据
            Message message = canalConnector.get(100);

            //判空
            if (message.getEntries().size() <= 0) {
                System.out.println("当前没有数据！休息一会！");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {

                //获取Entry集合
                List<CanalEntry.Entry> entries = message.getEntries();
                //遍历Entry集合
                for (CanalEntry.Entry entry : entries) {

                    //获取Entry类型
                    CanalEntry.EntryType entryType = entry.getEntryType();
                    //判断,只去RowData类型
                    if (CanalEntry.EntryType.ROWDATA.equals(entryType)) {

                        //获取表名
                        String tableName = entry.getHeader().getTableName();
                        //获取序列化的数据
                        ByteString storeValue = entry.getStoreValue();
                        //反序列化
                        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                        //获取事件类型
                        CanalEntry.EventType eventType = rowChange.getEventType();
                        //获取数据集合
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();

                        //根据表名以及事件类型处理数据rowDatasList
                        handler(tableName, eventType, rowDatasList);
                    }

                }

            }

        }

    }

    /**
     * 根据表名以及事件类型处理数据rowDatasList
     *
     * @param tableName    表名
     * @param eventType    事件类型
     * @param rowDatasList 数据集合
     */
    private static void handler(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) {

        //对于订单表而言,只需要新增数据
        if ("order_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {

            for (CanalEntry.RowData rowData : rowDatasList) {

                //创建JSON对象,用于存放多个列的数据
                JSONObject jsonObject = new JSONObject();

                for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                    jsonObject.put(column.getName(), column.getValue());
                }

                System.out.println(jsonObject.toString());
                //发送数据至Kafka
                MyKafkaSender.send(GmallConstant.GMALL_ORDER_INFO, jsonObject.toString());
            }
        }
    }
}
