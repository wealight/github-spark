package factory;

import com.alibaba.fastjson.JSONObject;
import utils.InputKeys;
import utils.SimpleKey;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by weishuxiao on 16/2/26.
 *
 * 用于最简单格式的日志字段解析
 * e.g.device_id,IMSI,PHONE_WIFI_MAC,
 * 只获取最外层字段,无法实现数组,嵌套形式的解析
 *
 */
public class SimpleLogParse implements InterfaceLogParse {
    private String valueString ="";


    @Override
    public List<String> parser(JSONObject jsonObject, InputKeys inputKeys){

        StringBuffer stringBuffer = new StringBuffer();

        if (inputKeys.isSimpleKeyEmpty()==false){
            for (SimpleKey simpleKey:inputKeys.getSimpleKeyList()){
                stringBuffer.append(getJsonValue(jsonObject,simpleKey.getKey())+"\t");
            }
           // stringBuffer.append("\n");
        }
        valueString = stringBuffer.deleteCharAt(stringBuffer.length()-1).toString();
        List<String> logList =(new ArrayList<>());
        logList.add(valueString);
        return logList;
    }

    /**
     * 根据key获取json当中的value
     * @param jsonObject json对象
     * @param key key值
     *
     * */
    public String getJsonValue(JSONObject jsonObject, String key){
        System.out.println(jsonObject.toJSONString());
//        System.out.println("!!!"+jsonObject.getString(key));
        if (jsonObject.containsKey(key)){
            return jsonObject.getString(key);
        }else {
            return "";
        }
    }

    /**
     * 返回解析之后的日志
     * */
    @Override
    public String getValueString() {
        return valueString;
    }

}
