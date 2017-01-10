package cn.wanda.ETLFactory.ETLParser

import cn.wanda.util.JsonKeys
import com.alibaba.fastjson.JSONObject

/**
  * Created by weishuxiao on 16/12/16.
  */
trait LogParserTrait {
  def parser(jsonObject: JSONObject, jsonKeys: JsonKeys): List[String]
}



