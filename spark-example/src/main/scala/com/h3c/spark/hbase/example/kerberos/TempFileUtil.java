package com.h3c.spark.hbase.example.kerberos;

import java.io.*;

public class TempFileUtil {
    /**
     * 创建临时文件
     * @param filename 源文件名字
     * @return 临时文件的全路径名
     */
    public static String createTempFile(String filename) {
        InputStream input = null;
        OutputStream out = null;
        File tempFile = null;
        try{
            input = TempFileUtil.class.getClassLoader().getResourceAsStream("conf" + File.separator + filename);
            String prefix = filename.substring(0,filename.lastIndexOf("."));
            String suffix = filename.substring(filename.lastIndexOf(".") + 1);
            tempFile =  File.createTempFile(prefix, suffix);
            out = new FileOutputStream(tempFile);
            int index = 0;
            byte[] buffer = new byte[1024];
            while ((index = input.read(buffer)) != -1){
                out.write(buffer, 0, index);
            }
            out.flush();
        }catch (IOException e){
            System.out.println("创建临时文件失败");
            e.printStackTrace();
            return null;
        }finally {
            if (input != null){
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    System.out.println("关闭输入流失败.");
                }
            }
            if (out != null){
                try{
                    out.close();
                }catch (IOException e){
                    e.printStackTrace();
                    System.out.println("关闭输出流失败.");
                }
            }
        }

        return tempFile.getAbsolutePath();
    }
}
