package utils;

import org.apache.commons.cli.*;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * 命令行参数解析类
 *
 * @author Administrator
 */
public class Cmd {
    private Options options;
    private String name;
    private CommandLine line;
    private List<CmdParam> keys = new ArrayList<CmdParam>();

    public Cmd(String cmdName) {
        this.name = "hadoop jar " + cmdName;
        this.options = new Options();
    }

    /**
     * 参数key以"--"开头，例如--reduce-num
     *
     * @param key
     * @param desc
     * @param flag
     */
    public void addParam(String key, String desc, boolean flag) {
        options.addOption(OptionBuilder.withLongOpt(key)
                .withDescription(desc)
                .hasArg()
                .withArgName("key=value")
                .create());
        CmdParam param = new CmdParam(key, flag, desc);
        keys.add(param);
    }

    /**
     * 参数key以"--"开头，例如--reduce-num，命令必须传入
     *
     * @param key
     * @param desc
     * @param flag
     */
    public void addParam(String key, String desc) {
        options.addOption(OptionBuilder.withLongOpt(key)
                .withDescription(desc)
                .hasArg()
                .withArgName("key=value")
                .create());
        CmdParam param = new CmdParam(key, true, desc);
        keys.add(param);
    }

    /**
     * 解析传入的参数
     *
     * @param args
     */
    public void parse(String[] args) {
        CommandLineParser parser = new PosixParser();
        try {
            this.line = parser.parse(options, args);
        } catch (ParseException e) {
//			e.printStackTrace();
            System.out.println("参数解析错误，请参考以下格式：");
            this.printHelp();
            System.exit(-1);
        }

        Iterator<Option> it = this.options.getOptions().iterator();
        List<String> unExistKeys = new ArrayList<String>();
        List<String> noMustKeys = new ArrayList<String>();
        boolean flag = true;
        for (CmdParam key : keys) {
            if (key.isFlag() && (!this.hasArg(key.getKey()) || "".equals(this.getArgValue(key.getKey()).trim()))) {
                unExistKeys.add(" --" + key.getKey() + "\t" + key.getDesc());
            } else if (!key.isFlag() && (!this.hasArg(key.getKey()) || "".equals(this.getArgValue(key.getKey()).trim()))) {
                noMustKeys.add(" --" + key.getKey() + "\t" + key.getDesc());
            }
        }

        if (unExistKeys.size() > 0) {
            System.out.println("以下参数必须在命令行中指定，而您未指定：");
            System.out.println("----------------------------------------------------");
            for (String key : unExistKeys) {
                System.out.println(key);
            }
            System.out.println("----------------------------------------------------");
            if (noMustKeys.size() > 0) {
                System.out.println("以下参数为可选参数：");
                System.out.println("----------------------------------------------------");
                for (String key : noMustKeys) {
                    System.out.println(key);
                }
                System.out.println("----------------------------------------------------");
            }

            System.exit(-1);
        }

        System.out.println("您使用的命令参数为：");
        System.out.println("----------------------------------------------------");
        for (CmdParam key : keys) {
            if (this.hasArg(key.getKey())) {
                System.out.println(" --" + key.getKey() + "=" + this.getArgValue(key.getKey()));
            }
        }
        System.out.println("----------------------------------------------------");
    }

    /**
     * 打印参数帮助
     */
    public void printHelp() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(this.name, this.options);
    }

    /**
     * 判断是否存在参数
     *
     * @param key
     * @return
     */
    public boolean hasArg(String key) {
        return line.hasOption(key);
    }

    /**
     * 得到key对应的值
     *
     * @param key
     * @return
     */
    public String getArgValue(String key) {
        return line.getOptionValue(key);
    }

    /**
     * 输入转换为map
     * @param argArray 输入数组 格式如--jobName=kafka_log_grep_ffan_m_log --date=2016-01-11
     * @param reg 所需使用的正则表达式
     *
     */

    public Map<String, String> args2String(String [] argArray, String reg){
        String str= Arrays.toString(argArray);
        Pattern pattern = Pattern.compile(reg);
        Matcher matcher = pattern.matcher(str);
        Map<String,String> map= new HashMap();
        while(matcher.find()){
            map.put(matcher.group(1),matcher.group(2));
        }
        return map;
    }
}
