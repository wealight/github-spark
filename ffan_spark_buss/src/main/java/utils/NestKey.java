package utils;

import java.io.Serializable;

/**
 * Created by weishuxiao on 16/2/24.
 */
public class NestKey  implements Serializable {
    private int order;
    private String [] layers;
    private String key;

    public NestKey(){}

    public NestKey(int order,String [] layers,String key){
        this.order=order;
        this.layers=layers;
        this.key=key;
    }

    public void setNestKey(int order,String [] layers,String key){
        this.order=order;
        this.layers=layers;
        this.key= key;
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

    public void setLayers(String[] layers) {
        this.layers = layers;
    }

    public String[] getLayers() {
        return layers;
    }
}
