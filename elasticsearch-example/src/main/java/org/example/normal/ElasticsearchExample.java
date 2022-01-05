package org.example.normal;

import org.elasticsearch.action.index.IndexResponse;
import org.example.kerberos.ElasticsearchClientUtil;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ElasticsearchExample {

    public static void main(String[] args) throws Exception{
        ElasticsearchClientUtil clientUtil = new ElasticsearchClientUtil();

        try {
            //向index_test001索引中写入数据
            Map<String, String> data = new HashMap<>();
            data.put("content", "test elasticsearch");
            IndexResponse indexResponse = clientUtil.writeIndex("index_test001", data);
            System.out.println(indexResponse.getResult().toString());
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            try{
                clientUtil.close();
            }catch (IOException e){
                e.printStackTrace();
            }
        }
    }
}
