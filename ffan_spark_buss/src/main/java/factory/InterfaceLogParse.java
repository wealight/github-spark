package factory;

import com.alibaba.fastjson.JSONObject;
import utils.InputKeys;

import java.util.List;

/**
 * Created by weishuxiao on 16/2/26.
*/
public interface InterfaceLogParse {

     List<String> parser(JSONObject jsonObject, InputKeys inputKeys);

     String getValueString();
}
