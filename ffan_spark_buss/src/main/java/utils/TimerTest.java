package utils;

/**
 * Created by weishuxiao on 16/9/20.
 */

import java.util.Timer;

/**
 * Created by weishuxiao on 16/9/19.
 */
public class TimerTest {
    public static void main(String[] args) {
        // TODO todo.generated by zoer
        Timer timer = new Timer();
        timer.schedule(new MyTask(), 1000, 2000);

        while (true){
            MyTask.var=MyTask.var+1;
            System.out.println("Main Thread: var="+MyTask.var);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}


class MyTask extends java.util.TimerTask {

    static int var=0;
    @Override
    public void run() {
        var=var+1;
        System.out.println("TimerTask Thread: var="+var);
    }
}