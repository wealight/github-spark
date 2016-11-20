package utils;

/**
 * @author Administrator
 */
public class CmdParam {
    private String key;
    private boolean flag;
    private String desc;

    public CmdParam(String key, boolean flag, String desc) {
        super();
        this.key = key;
        this.flag = flag;
        this.desc = desc;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public boolean isFlag() {
        return flag;
    }

    public void setFlag(boolean flag) {
        this.flag = flag;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }


}
