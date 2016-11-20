package utils;

import java.io.Serializable;

/**
 * Created by weishuxiao on 16/2/24.
 */
public class ArrayKey implements Serializable {
    private int order;
    private String key;
    private String arrayName;

    public ArrayKey(){}

    public ArrayKey(int order,String arrayName,String key){
        this.order = order;
        this.key = key;
        this.arrayName = arrayName;
    }

    public void setArrayKey(int order,String arrayName,String key){
        this.order = order;
        this.key = key;
        this.arrayName = arrayName;
    }

    public void setOrder(int order) {
        this.order = order;
    }

    public int getOrder() {
        return order;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    public void setArrayName(String arrayName) {
        this.arrayName = arrayName;
    }

    public String getArrayName() {
        return arrayName;
    }
}
