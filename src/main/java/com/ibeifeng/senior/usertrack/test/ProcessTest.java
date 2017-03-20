package com.ibeifeng.senior.usertrack.test;

import java.io.IOException;

/**
 * Created by ibf on 12/13.
 */
public class ProcessTest {
    public static void main(String[] args) {
        String shellPath = "/home/beifeng/o2o/test.sh";
        String argParam = args.length == 0 ? "spark" : args[0];
        // eg: sh /home/beifeng/o2o/test.sh spark
        String cmd = "sh " + shellPath + " " + argParam;

        try {
            // 执行shell命令
            Process process = Runtime.getRuntime().exec(cmd);
            // 等待命令执行完成
            int exitValue = process.waitFor();
            if (exitValue == 0) {
                System.out.println("Success");
            } else {
                System.out.println("Failure_" + exitValue);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
