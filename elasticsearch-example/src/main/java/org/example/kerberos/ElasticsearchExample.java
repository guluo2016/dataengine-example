package org.example.kerberos;

import java.io.IOException;

public class ElasticsearchExample {

    public static void main(String[] args) throws Exception{
        ElasticsearchClientUtil clientUtil = new ElasticsearchClientUtil();

        try {
            //查询索引index_test001
            clientUtil.searchIndex("index_test001");
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
