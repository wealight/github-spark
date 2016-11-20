package utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * Created by weishuxiao on 16/1/11.
 */
public class HdfsOperator {

    /**
     * 将字符串写入到指定hdfs文件.
     *
     * @param file 所写入的文件
     * @param words 待写入字符串
     * @param conf Configuration
     */

    public void write2HDFS(Configuration conf,String file, String words) throws IOException, URISyntaxException
    {
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(file);
        FSDataOutputStream out = fs.create(path,true);   //创建文件
        out.write(words.getBytes("UTF-8"));
        out.close();
    }

    /**
     * 从hdfs文件当中读取文件内容.
     *
     * @param file hdfs文件
     * @param conf Configuration
     *
     */
    public void readHDFS(Configuration conf,String file) throws IOException
    {
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(file);
        FSDataInputStream in = fs.open(path);
        IOUtils.copyBytes(in, System.out, 4096, true);
    }

    /**
     * 当写入路径已经存在时,先将该路径清除
     *
     * @param dir 写入路径
     * @param conf Configuration
     */
    public void dirClear(Configuration conf,String dir) throws IOException
    {
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(dir);
        if(fs.exists(path)){
            System.out.println(dir+"日志已经存在!");
            fs.delete(path,true);
            System.out.println("删除原先日志"+dir);
        }
        //fs.mkdirs(path);
    }

    /**
     * 获取当前时间
     */
    public static String getTime(){
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");
        String current_time= formatter.format(new Date());
        return current_time;
    }

    /**
     * map对象转为字符串
     *
     * @param map
     * @param prefixArray 输入的前缀数组
     */
    public String map2String(Map<String, Long> map,String separator,String ... prefixArray){

        String prefixs="";
        for (String element:prefixArray){
            prefixs=prefixs+element+separator;
        }
        StringBuffer sb = new StringBuffer();
        for (Map.Entry<String, Long>entry : map.entrySet()) {
            sb.append(prefixs+entry.getKey() + separator + entry.getValue()+'\n');
        }
        return sb.toString();
    }
}