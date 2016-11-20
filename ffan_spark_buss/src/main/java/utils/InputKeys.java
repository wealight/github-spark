package utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by weishuxiao on 16/2/24.
 */
public class InputKeys implements Serializable {

    private final String SIMPLE_REG="^(\\w+)$";
    private final String NEST_REG="^(\\w+\\.){1,}(\\w+.)$";
    private final String ARRAY_REG="^(\\w+)\\[(\\w+)\\]$";

    private Pattern patternSimple = Pattern.compile(SIMPLE_REG);
    private Pattern patternNest = Pattern.compile(NEST_REG);
    private Pattern patternArray = Pattern.compile(ARRAY_REG);



    private List<SimpleKey> simpleKeyList= new ArrayList();
    private List<NestKey> nestKeyList = new ArrayList();
    private List<ArrayKey> arrayKeyList= new ArrayList();

    private TreeMap<Integer,String> keysTreeMap = new TreeMap<Integer,String>();

    /**
     * 解析输入字段
     * @param keyString 输入字段  格式为 "key1,key2.value1,key3[value2]"
     * */
    public void keysParser(String keyString){

        Matcher matcherSimple;
        Matcher matcherNest;
        Matcher matcherArray;

        String[] keys=keyString.trim().split(",");
        for (int i=0;i<keys.length;i++){
            String key = keys[i];
            matcherSimple = patternSimple.matcher(key);
            matcherNest = patternNest.matcher(key);
            matcherArray = patternArray.matcher(key);

            if (matcherSimple.find()){
                simpleKeyList.add(new SimpleKey(i,key));
                keysTreeMap.put(i,key);

            }else if (matcherNest.find()){
                String [] layers = key.trim().split("\\.");
                nestKeyList.add(new NestKey(i,layers,layers[layers.length-1]));
                keysTreeMap.put(i,layers[layers.length-1]);

            }else if (matcherArray.find()){
                arrayKeyList.add(new ArrayKey(i,matcherArray.group(1),matcherArray.group(2)));
                keysTreeMap.put(i,matcherArray.group(2));
            }else {
                System.out.println("错误格式字段输入:"+ Arrays.toString(keys));
            }
        }
    }


    public List<SimpleKey> getSimpleKeyList() {
        return simpleKeyList;
    }

    public List<NestKey> getNestKeyList() {
        return nestKeyList;
    }

    public List<ArrayKey> getArrayKeyList() {
        return arrayKeyList;
    }

    public boolean isSimpleKeyEmpty(){
        if (simpleKeyList.size() == 0){
            return true;
        }else {
            return false;
        }
    }

    public boolean isNestKeyEmpty(){
        if (simpleKeyList.size() == 0){
            return true;
        }else {
            return false;
        }
    }

    public boolean isArrayKeyEmpty(){
        if (arrayKeyList.size() == 0){
            return true;
        }else {
            return false;
        }
    }

    /**
     *将输入解析字段的字段名称以字符串的形式返回
     * "key1,key2.value1,key3[value2]"  返回结果为 "key1,value1,value2"
     * */
    public String getKeysString(){

        StringBuffer sb = new StringBuffer();

        for (int i=0;i<keysTreeMap.size();i++){
            if (i<keysTreeMap.size()-1){
                sb.append(keysTreeMap.get(i)+",");
            }else {
                sb.append(keysTreeMap.get(i));
            }
        }
        return sb.toString();
    }

    /**
     *将输入解析字段的字段名称以字符串的形式返回
     * "key1,key2.value1,key3[value2]"  返回结果为 "nvl(key1),nvl(value1),nvl(value2)"
     * */
    public String getNvlKeysString(String def){

        StringBuffer sb = new StringBuffer();

        for (int i=0;i<keysTreeMap.size();i++){
            if (i<keysTreeMap.size()-1){
                sb.append("if("+keysTreeMap.get(i)+"='',\'"+def+"\',"+keysTreeMap.get(i)+") as "+keysTreeMap.get(i)+",");
            }else {
                sb.append("if("+keysTreeMap.get(i)+"='',\'"+def+"\',"+keysTreeMap.get(i)+") as "+keysTreeMap.get(i));
            }
        }
        return sb.toString();
    }


    /**
     *将输入解析字段的字段名称以字符串的形式返回
     * "key1,key2.value1,key3[value2]"  返回结果为 "nvl(key1),nvl(value1),nvl(value2)"
     * */
    public String[] getNvlKeysStringArray(String def){
        List<String> list = new ArrayList();
        for (int i=0;i<keysTreeMap.size();i++){
            list.add(i,"nvl("+keysTreeMap.get(i)+",\'"+def+"\') as "+keysTreeMap.get(i));
        }
        String[] arr=(String[])list.toArray(new String[list.size()]);
        System.out.println(Arrays.toString(arr));
        return arr;
    }
}
