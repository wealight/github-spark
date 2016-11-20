package utils;

import java.io.Serializable;

/**
 * Created by weishuxiao on 16/2/24.
 */
public class SimpleKey  implements Serializable {
    private int order;
    private String key;

    public SimpleKey(){}

    public SimpleKey(int order,String key){
        this.order=order;
        this.key=key;
    }

    public void setSimpleKey(int order,String key){
        this.order=order;
        this.key=key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    public void setOrder(int order) {
        this.order = order;
    }

    public int getOrder() {
        return order;
    }
}
