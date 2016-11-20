package factory;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import utils.ArrayKey;
import utils.InputKeys;
import utils.NestKey;
import utils.SimpleKey;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;

/**
 * Created by weishuxiao on 16/2/24.
 */
public class GeneralLogParse implements InterfaceLogParse,Serializable {

    private String valueString ="";
    private StringBuffer stringBuffer = new StringBuffer();
    private List<String> logsList = new ArrayList<>();

    @Override
    public List<String> parser(JSONObject jsonObject, InputKeys inputKeys){

        List<TreeMap<Integer,String>> treeMapList = new ArrayList();
        TreeMap<Integer,String> subMap = new TreeMap();

        if (inputKeys.isSimpleKeyEmpty()==false){
            for (SimpleKey simpleKey:inputKeys.getSimpleKeyList()){
                subMap.put(simpleKey.getOrder(),getJsonValue(jsonObject,simpleKey.getKey()));
            }
        }

        if (inputKeys.isNestKeyEmpty()==false){
            JSONObject subJsonObject = new JSONObject();
            for (NestKey nestKey:inputKeys.getNestKeyList()){
                subJsonObject = (JSONObject) jsonObject.clone();
                for (int j=0;j<nestKey.getLayers().length-1;j++){
                    try {
                        subJsonObject = subJsonObject.getJSONObject(nestKey.getLayers()[j]);
                    }catch (NullPointerException e){
                        //System.out.println("item.type---->json当中没有该layer字段:"+Arrays.toString(nestKey.getLayers()));
                        subMap.put(nestKey.getOrder(),"");
                    }
                }

                try {
                    subMap.put(nestKey.getOrder(),getJsonValue(subJsonObject,nestKey.getKey()));
                }catch (NullPointerException e){
                    //System.out.println("item.type---->json当中没有该字段:"+nestKey.getKey());
                    subMap.put(nestKey.getOrder(),"");
                }
            }
        }

        if (!inputKeys.isArrayKeyEmpty()){
            JSONArray jsonArray = jsonObject.getJSONArray(inputKeys.getArrayKeyList().get(0).getArrayName());

            try {
                int size = jsonArray.size();
                for (int k=0;k<size;k++){
                    JSONObject jsonElement = new JSONObject();
                    jsonElement = jsonArray.getJSONObject(k);
                    TreeMap<Integer, String> treeMap = new TreeMap<>();
                    for (ArrayKey arrayKey:inputKeys.getArrayKeyList()){
                        if (arrayKey.getKey().equals("orig_info")){
                            treeMap.put(arrayKey.getOrder(),jsonElement.toString());
                        }else {
                            treeMap.put(arrayKey.getOrder(),getJsonValue(jsonElement,arrayKey.getKey()));
                        }
                    }
                    treeMapList.add(treeMap);
                }
            }catch (NullPointerException e){
                TreeMap<Integer, String> treeMap = new TreeMap<>();
                for (ArrayKey arrayKey:inputKeys.getArrayKeyList()){
                    treeMap.put(arrayKey.getOrder(),"");
                    //System.out.println("item[key]---->json当中没有该字段:"+arrayKey.getArrayName()+"["+arrayKey.getKey()+"]");
                }
                treeMapList.add(treeMap);
            }
        }
//        setValueString(inputKeys,subMap,treeMapList);
//        System.out.println(valueString);
        setLogsList(inputKeys,subMap,treeMapList);
        valueString = stringBuffer.toString();
        return logsList;
    }


    /**
     * 根据key获取json当中的value
     * @param jsonObject json对象
     * @param key key值
     *
     * */
    public String getJsonValue(JSONObject jsonObject,String key){
        if (jsonObject.containsKey(key)){
            return jsonObject.getString(key);
        }else {
            return "-";
        }
    }


    /**
     * map对象转为字符串
     *
     * @param treeMap
     *
     */
    public String map2String(TreeMap<Integer, String> treeMap, String separator){

        StringBuffer sb = new StringBuffer();
        Iterator it = treeMap.keySet().iterator();

        while (it.hasNext()) {
            sb.append(treeMap.get(it.next())+ separator);
        }

        return sb.deleteCharAt(sb.length()-1).toString();//删除最后一个空的分隔符
    }

    @Override
    public String getValueString() {
        return valueString;
    }

    /**
     * 拼接所有的获取字段,获得最终输出
     *
     * @param inputKeys 解析后的待获取字段
     * @param subMap 简单字段以及嵌套字段组成的treemap
     * @param treeMapList 数组字段组成的List<TreeMap<Integer,String>> 对象
     *
     */
    public void setValueString(InputKeys inputKeys,TreeMap<Integer, String> subMap,List<TreeMap<Integer,String>> treeMapList){
        if (inputKeys.isArrayKeyEmpty()==true){
            stringBuffer.append(map2String(subMap,"\t"));
        }else {
            TreeMap<Integer, String> treeMap = new TreeMap<>();
            treeMap.putAll(subMap);
            for (int i=0;i<treeMapList.size();i++){
                treeMap.putAll(treeMapList.get(i));
                stringBuffer.append(map2String(treeMap,"\t")+"\n");
            }
        }
    }

    /**
     * 拼接所有的获取字段,获得最终输出
     *
     * @param inputKeys 解析后的待获取字段
     * @param subMap 简单字段以及嵌套字段组成的treemap
     * @param treeMapList 数组字段组成的List<TreeMap<Integer,String>> 对象
     *
     */
    public void setLogsList(InputKeys inputKeys,TreeMap<Integer, String> subMap,List<TreeMap<Integer,String>> treeMapList){
        if (inputKeys.isArrayKeyEmpty()==true){
            logsList.add(map2String(subMap,"\t"));
        }else {
            TreeMap<Integer, String> treeMap = new TreeMap<>();
            treeMap.putAll(subMap);
            for (int i=0;i<treeMapList.size();i++){
                treeMap.putAll(treeMapList.get(i));
                logsList.add(map2String(treeMap,"\t"));
            }
        }
    }

    public List<String> getLogsList() {return logsList;}
}
