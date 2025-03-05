/*
 * Copyright  2005 PB Consult Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package com.pb.tlumip.ao;

import com.pb.common.logging.LogServer;
import com.pb.common.util.ResourceUtil;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ResourceBundle;

/**
 * This class starts the nodes, the cluster and then the application
 * that is passed in as an argument by writing to a command file that the
 * FileMonitor class is monitoring.  The FileMonitor must be running on the
 * machines that will host the daf nodes before this class can be called.
 * <p>
 * StartDafApplication will check for the existence of a file called
 * 'appNameDone'.  When this file appears, the class will exit.
 * Before exiting, this class will shut down the nodes.
 *
 * @author Christi Willison
 * @version Jun 7, 2004
 */
public class StartDafApplication {
    private static Logger logger = Logger.getLogger(StartDafApplication.class);
    private volatile ResourceBundle rb = null;
    private long startNodeSleepTime = 15000;
    private long startClusterApplicationSleepTime = 15000;
    private long fileCheckSleepTime = 55;
    private int t;
    //String pathPrefix;
    String scenarioOutputs;
    String doneFilePath;
    File commandFile;
    File errorFile; //the error file
    File appDone;   //the file name should be something like "pidaf_done.txt"
    String appName; //should be in all lower-case
    String scenarioName; //should be the same as designated on the command line
    String nodeName; //name of the node that will start the cluster - can be any node
    String rootDir;

    public StartDafApplication(String appName, String rootDir, String scenarioOutputs, int timeInterval){
        this(appName, "node0", rootDir, scenarioOutputs, timeInterval);
    }

    public StartDafApplication(String appName, ResourceBundle rb, String nodeName, int timeInterval) {
        this(appName,nodeName,ResourceUtil.getProperty(rb,"root.dir"),ResourceUtil.getProperty(rb,"scenario.outputs.relative"),timeInterval);
        this.rb = rb;
        if (rb.containsKey("daf.start.node.sleep.time"))
            startNodeSleepTime = (long) ResourceUtil.getIntegerProperty(rb,"daf.start.node.sleep.time");
        if (rb.containsKey("daf.start.cluster.sleep.time"))
            startClusterApplicationSleepTime = (long) ResourceUtil.getIntegerProperty(rb,"daf.start.cluster.sleep.time");
        if (rb.containsKey("daf.done.file.check.sleep.time"))
            fileCheckSleepTime = (long) ResourceUtil.getIntegerProperty(rb,"daf.done.file.check.sleep.time");
    }

    public StartDafApplication(String appName,ResourceBundle rb, int timeInterval) {
        this(appName,rb,"node0",timeInterval);
    }

    public StartDafApplication(String appName, String nodeName, String rootDir, String scenarioOutputs, int timeInterval){
        //this.rb = rb;
        this.appName = appName;
        this.nodeName = nodeName;
        this.scenarioOutputs = scenarioOutputs;
        this.rootDir = rootDir;

        this.t = timeInterval;

        //this.rootDir =  ResourceUtil.getProperty(rb, "root.dir");
        //this.scenarioName = ResourceUtil.getProperty(rb, "scenario.name");

        //this.pathPrefix = rootDir + "scenario_" + scenarioName+ "/";

        this.errorFile = new File(rootDir + LogServer.CLIENTLOG_NAME);
    }

    private File getCommandFile(String cmdFilePath){
        File cmdFile = new File(cmdFilePath+"/commandFile.txt");
        logger.info("Cmd File: " + cmdFile);
        if(!cmdFile.exists()){
            logger.info("The file used by the FileMonitor class does not exist - creating file");
            try {
                cmdFile.createNewFile();
            } catch (IOException e) {
                logger.fatal(cmdFile.getAbsolutePath() + " could not be created");
                e.printStackTrace();
                System.exit(10);
            }
        }
        logger.info("Command file has been found");
        return cmdFile;
    }

    private void deleteAppDoneFile(File doneFile){        
        if(doneFile.exists()){
            logger.info("Deleting the zz"+appName+"_done.txt file");
            doneFile.delete();
            if(doneFile.exists()) logger.info("zz" + appName+"_done.txt file still exists");
        }
    }

    private void writeCommands(){
        writeCommandToCmdFile(Entry.START_NODE);
        try {
            Thread.sleep(startNodeSleepTime);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        writeCommandToCmdFile(Entry.START_CLUSTER);
        try {
            Thread.sleep(startClusterApplicationSleepTime);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        writeCommandToCmdFile(Entry.START_APPLICATION);

        logger.info("Wait here for the application to finish");
        long waitTime = System.currentTimeMillis();
        waitForAppDoneOrErrorCondition();
        logger.info("Application has finished. Time in seconds: "+(System.currentTimeMillis()-waitTime)/1000.0);
    }



    private void writeCommandToCmdFile(String entry){
        logger.info("Writing '"+ entry + "' to command file");
        PrintWriter writer;
        try {
            writer = new PrintWriter(new FileWriter(commandFile));

            if(entry.equals(Entry.START_CLUSTER)){
                writer.println(Entry.START_CLUSTER);
                writer.println(nodeName);

            }else if(entry.equals(Entry.START_APPLICATION)){
                writer.println(Entry.START_APPLICATION);
                writer.println(nodeName);
                writer.println(appName.toLowerCase());

            }else{
                writer.println(entry); //all other commands have just a single entry with no arguments
            }

            writer.close();

        } catch (IOException e) {
            logger.fatal("Could not open command file or was not able to write to it - check file properties");
            e.printStackTrace();
        }

    }

    private void waitForAppDoneOrErrorCondition(){
        boolean stopRequested = false;

        while(! stopRequested) {
            try {
                Thread.sleep(fileCheckSleepTime);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            //Check if an error has been written to LogServer file
            if (errorFile != null && errorFile.length() > 0) {
                logger.fatal("leaving waitForAppDoneOrErrorCondition() because an error was found");
                break;
            }

            //Check that the file exists
            if (appDone.exists()) {
                stopRequested=true;
            }
        }
    }

    private void cleanUpAndExit(){
        writeCommandToCmdFile(Entry.STOP_NODE);
    }

    public void run(){
        //get the path to the command file and make sure the file exists
        String cmdPath = (rb == null || !rb.containsKey("daf.command.file.dir")) ?
                                       (rootDir + "/" + scenarioOutputs + "/ops") :
                                        ResourceUtil.getProperty(rb, "daf.command.file.dir");
        logger.info("CommandFile Path: "+ cmdPath);
        commandFile = getCommandFile(cmdPath);

        //construct the path to the $appName_done.txt file
        //and delete the file if it already exists
       // int appIndex = appName.indexOf("daf");
        //String doneFile = pathPrefix + "t" + t + "/" + appName.substring(0,appIndex) + "/" + appName + "_done.txt";
        String doneFile = (rb == null || !rb.containsKey("sdt.done.file")) ?
                                         (rootDir + "/" + scenarioOutputs + "/ops/zz" +  appName + "_done.txt") :
                                         ResourceUtil.getProperty(rb, "sdt.done.file");
        //String doneFile = cmdPath + "/" + appName + "_done.txt";
        logger.info("DoneFile Path: " + doneFile);
        appDone = new File(doneFile);
        deleteAppDoneFile(appDone);

        //begin the daf application by writing the correct
        //commands to the command file
        logger.info("Starting nodes, cluster and application.  Waiting " + startNodeSleepTime + " ms for nodes to start");
        writeCommands();

        logger.info("Ending application");
        //end daf application by writing 'StopNode' into the command file
        cleanUpAndExit();

    }




    public static void main(String[] args) {
        String appName = args[0];
        String nodeName = args[1];
        int t = Integer.parseInt(args[2]);
        ResourceBundle rb = ResourceUtil.getPropertyBundle(new File("/models/tlumip/scenario_msgCrazy/ao.properties"));
        logger.info("appName: "+ appName);
        logger.info("nodeName: "+ nodeName);
        logger.info("timeInterval " + t);
        //StartDafApplication appRunner = new StartDafApplication(appName,nodeName,t, rb);
        //String appName, String rootDir, String scenarioOutputs, int timeInterval
       // appRunner.run();


    }

}
