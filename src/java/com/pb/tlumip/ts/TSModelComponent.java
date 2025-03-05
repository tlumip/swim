/*
 * Copyright 2006 PB Consult Inc.
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
package com.pb.tlumip.ts;

import com.pb.common.rpc.DafNode;
import com.pb.common.util.DosCommand;
import com.pb.common.util.ResourceUtil;
import com.pb.models.reference.ModelComponent;
import com.pb.models.utils.StatusLogger;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.util.ResourceBundle;
import java.util.MissingResourceException;


/**
 * This class is used for ...
 * Author: Christi Willison
 * Date: Mar 15, 2007
 * Email: willison@pbworld.com
 * Created by IntelliJ IDEA.
 */
public class TSModelComponent extends ModelComponent {

    Logger logger = Logger.getLogger(TSModelComponent.class);
    
//    static final String WINDOWS_CMD_TARGET = "windows.cmd";
//    static final String PYTHON_CMD_TARGET = "python.cmd";
    static final String PYTHON_PROGRAM_TARGET = "python.source";
    static final int COUNT_YEAR = 1998;

    public static final String ONLY_CALCULATE_DEMAND_PROPERTY = "ts.only.calculate.demand";
    
    
    private String configFileName;
    private boolean dailyModel;

    TS ts;

    /**
     * If running in DAF mode, the configFileName will be something other than "null".
     * If the configFileName is null, then TS will run monolithically.
     * @param appRb is the TS component specific properties file ResourceBundle.
     * @param globalRb is the global model properties file ResourceBundle.
     * @param configFileName is the name of a DAF 3 configuration file (.groovy) that defines machine addresses and handler classes.
     * @param dailyModelFlag is a Boolean that if true, causes all 4 assignment periods to be run.  If false or null, only amPeak and
     *        mdOffPeak periods are assigned.  FullModel runs are intended for base year and final year to produce full daily
     *        assignment results while intermediate model years require assignment procedures only for the purpose of producing
     *        representative peak and off-peak travel skim matrices for spatial models.
     */
    public TSModelComponent( ResourceBundle appRb, ResourceBundle globalRb, String configFileName, Boolean dailyModelFlag ){
		setResourceBundles(appRb, globalRb);    //creates a resource bundle as a class attribute called appRb.
        this.configFileName = configFileName;
        
        dailyModel = false;
        if ( dailyModelFlag != null ) {
            dailyModel = dailyModelFlag.booleanValue();
        }
        
    }

    
    public void startModel(int baseYear, int timeInterval){
        logger.info("Config file name: " + configFileName);
        if ( configFileName != null ) {
            try {
                DafNode.getInstance().initClient(configFileName);
            }
            catch (MalformedURLException e) {
                logger.error( "MalformedURLException caught initializing a DafNode.", e);
            }
            catch (Exception e) {
                logger.error( "Exception caught initializing a DafNode.", e);
            }

        }
        ts = new TS(appRb, globalRb);
        //if demand only is set, then just calculate demand matrices and exit
        if (appRb.containsKey(ONLY_CALCULATE_DEMAND_PROPERTY) && Boolean.parseBoolean(appRb.getString(ONLY_CALCULATE_DEMAND_PROPERTY))) {
            createAndWriteHwyAndTransitDemandMatrices("ampeak");
            createAndWriteHwyAndTransitDemandMatrices("mdoffpeak");
            createAndWriteHwyAndTransitDemandMatrices("pmpeak");
            createAndWriteHwyAndTransitDemandMatrices("ntoffpeak");
            return;
        }

        // amPeak and mdOffPeak periods are always run 
        assignAndSkimHwyAndTransit("ampeak");
        assignAndSkimHwyAndTransit("mdoffpeak");

        // pmPeak and ntOffPeak periods are run only if a daily model is required, and it's not SKIMS_ONLY mode.
        if ( dailyModel && ! ts.SKIM_ONLY ) {
            assignAndSkimHwyAndTransit("pmpeak");
            assignAndSkimHwyAndTransit("ntoffpeak");
        } else {
            //these won't write if the demand output file is not defined, so calling them is ok here
            createAndWriteHwyAndTransitDemandMatrices("pmpeak");
            createAndWriteHwyAndTransitDemandMatrices("ntoffpeak");
        }

    }

    private void createAndWriteHwyAndTransitDemandMatrices(String period) {
        //this whole method just pulls out what is needed from various places in TS to create a demand handler
        // object to create demand matrices for writing out
        String demandOutputFile = null;
        try {
            demandOutputFile = appRb.getString("demand.output.filename");
        }
        catch (MissingResourceException e) {
            // do nothing, filename can be null.
        }
        if (ts.SKIM_ONLY || demandOutputFile == null)
            return; //if skim only or not writing no reason to create demand matrices

        NetworkHandlerIF nh = NetworkHandler.getInstance(configFileName);

        if ( nh.getStatus() )
            logger.info ( nh.getClass().getCanonicalName() + " instance created, and handler is active." );

        try {
            nh.setRpcConfigFileName( configFileName );
            if ( ts.setupHighwayNetwork( nh, ResourceUtil.changeResourceBundleIntoHashMap(appRb), ResourceUtil.changeResourceBundleIntoHashMap(globalRb), period ) < 0 )
                throw new Exception();
            logger.info ("created " + period + " Highway NetworkHandler object: " + nh.getNodeCount() + " highway nodes, " + nh.getLinkCount() + " highway links." );

        }
        catch (Exception e) {
            logger.error ( "Exception caught setting up network in " + nh.getClass().getCanonicalName(), e );
            System.exit(-1);
        }


        double ptSampleRate = 1.0;
        String rateString = globalRb.getString( "pt.sample.rate" );
        if ( rateString != null )
            ptSampleRate = Double.parseDouble( rateString );

        String timePeriod = nh.getTimePeriod();
        int startHour = 0;
        int endHour = 0;
        if ( timePeriod.equalsIgnoreCase( "ampeak" ) ) {
            // get am peak period definitions from property files
            startHour = Integer.parseInt( globalRb.getString( "am.peak.start") );
            endHour = Integer.parseInt( globalRb.getString( "am.peak.end" ) );
        }
        else if ( timePeriod.equalsIgnoreCase( "pmpeak" ) ) {
            // get pm peak period definitions from property files
            startHour = Integer.parseInt( globalRb.getString( "pm.peak.start") );
            endHour = Integer.parseInt( globalRb.getString( "pm.peak.end" ) );
        }
        else if ( timePeriod.equalsIgnoreCase( "mdoffpeak" ) ) {
            // get md off-peak period definitions from property files
            startHour = Integer.parseInt( globalRb.getString( "md.offpeak.start") );
            endHour = Integer.parseInt( globalRb.getString( "md.offpeak.end" ) );
        }
        else if ( timePeriod.equalsIgnoreCase( "ntoffpeak" ) ) {
            // get nt off-peak period definitions from property files
            startHour = Integer.parseInt( globalRb.getString( "nt.offpeak.start") );
            endHour = Integer.parseInt( globalRb.getString( "nt.offpeak.end" ) );
        }

        DemandHandlerIF dh = DemandHandler.getInstance(nh.getRpcConfigFileName());
        dh.setup(nh.getUserClassPces(), demandOutputFile,
                globalRb.getString("sdt.person.trips"), globalRb.getString("ldt.vehicle.trips"), ptSampleRate,
                globalRb.getString("ct.truck.trips"), globalRb.getString("et.truck.trips"),
                startHour, endHour, timePeriod, nh.getNumCentroids(), nh.getNumUserClasses(),
                nh.getIndexNode(), nh.getNodeIndex(), nh.getAlphaDistrictIndex(), nh.getDistrictNames(),
                nh.getAssignmentGroupChars(), nh.getHighwayModeCharacters(), nh.userClassesIncludeTruck());
        dh.buildHighwayDemandObject();



        //This will return a local network handler.  Jim needs to
        //test the remote network handler for transit skim building.
        NetworkHandlerIF nh_new = NetworkHandler.getInstance(null);
        try {
            if ( ts.setupHighwayNetwork( nh_new, ResourceUtil.changeResourceBundleIntoHashMap(appRb), ResourceUtil.changeResourceBundleIntoHashMap(globalRb), period ) < 0 )
                throw new Exception();
            logger.info ("created " + period + " Highway NetworkHandler object: " + nh_new.getNodeCount() + " highway nodes, " + nh_new.getLinkCount() + " highway links." );
        }
        catch (Exception e) {
            logger.error ( "Exception caught setting up network in " + nh_new.getClass().getCanonicalName(), e );
            System.exit(-1);
        }

        StatusLogger.logText("TS","Running transit assignment and skimming for " + period);
        ts.assignAndSkimTransit ( nh_new, appRb,globalRb,true);
    }



    private void assignAndSkimHwyAndTransit(String period){

        NetworkHandlerIF nh = NetworkHandler.getInstance(configFileName);

        
        if ( nh.getStatus() ) {
            logger.info ( nh.getClass().getCanonicalName() + " instance created, and handler is active." );
        }

        try {
            nh.setRpcConfigFileName( configFileName );
            if ( ts.setupHighwayNetwork( nh, ResourceUtil.changeResourceBundleIntoHashMap(appRb), ResourceUtil.changeResourceBundleIntoHashMap(globalRb), period ) < 0 )
                throw new Exception();
            logger.info ("created " + period + " Highway NetworkHandler object: " + nh.getNodeCount() + " highway nodes, " + nh.getLinkCount() + " highway links." );
            
        }
        catch (Exception e) {
            logger.error ( "Exception caught setting up network in " + nh.getClass().getCanonicalName(), e );
            System.exit(-1);
        }


        // if SKIM_ONLY is false (set by skimOnly.flag TS property map key missing or set equal to false)
        // then skip the trip assignment step.
        if ( ! ts.SKIM_ONLY ) {
            StatusLogger.logText("TS","Running highway assignment for " + period);
            ts.runHighwayAssignment( nh );
        }
        

        
        //This will return a local network handler.  Jim needs to
        //test the remote network handler for transit skim building.
        NetworkHandlerIF nh_new = NetworkHandler.getInstance(null);
        try {
            if ( ts.setupHighwayNetwork( nh_new, ResourceUtil.changeResourceBundleIntoHashMap(appRb), ResourceUtil.changeResourceBundleIntoHashMap(globalRb), period ) < 0 )
                throw new Exception();
            logger.info ("created " + period + " Highway NetworkHandler object: " + nh_new.getNodeCount() + " highway nodes, " + nh_new.getLinkCount() + " highway links." );
        }
        catch (Exception e) {
            logger.error ( "Exception caught setting up network in " + nh_new.getClass().getCanonicalName(), e );
            System.exit(-1);
        }


        StatusLogger.logText("TS","Running highway skimming for " + period);
        ts.loadAssignmentResults(nh_new, appRb);
        runLinkSummaries ( nh_new );
        
        
        char[] hwyModeChars = nh_new.getUserClasses();
        ts.writeHighwaySkimMatrices ( nh_new, hwyModeChars );


        StatusLogger.logText("TS","Running transit assignment and skimming for " + period);
        ts.assignAndSkimTransit ( nh_new,  appRb, globalRb, false);
        
    }

        
    
//    private void runLinkSummaries ( NetworkHandlerIF nh_new ) {
//
//        String assignmentPeriod = nh_new.getTimePeriod();
//
//        String linkSummaryFileName = null;
//
//        // get output filename for link summary statustics report written by python program
//        linkSummaryFileName = (String)appRb.getString( "linkSummary.fileName" );
//        if ( linkSummaryFileName != null ) {
//
//            int index = linkSummaryFileName.indexOf(".");
//            if ( index < 0 ) {
//                linkSummaryFileName += "_" + assignmentPeriod;
//            }
//            else {
//                String extension = linkSummaryFileName.substring(index);
//                linkSummaryFileName = linkSummaryFileName.substring(0, index);
//                linkSummaryFileName += "_" + assignmentPeriod + extension;
//            }
//
//            String a2bFileName = (String)globalRb.getString( "alpha2beta.file" );
//            String countsFileName = (String)appRb.getString( "counts.file" );
//
//            String winCmdLocation = appRb.getString( WINDOWS_CMD_TARGET );
//
//            String pythonCommand = appRb.getString( PYTHON_CMD_TARGET );
//            String pythonSrc = appRb.getString( PYTHON_PROGRAM_TARGET );
//
//            String commandString = String.format ( "%s %s %d %s %s %s %s %s %d", pythonCommand, pythonSrc, COUNT_YEAR, assignmentPeriod, linkSummaryFileName, a2bFileName, countsFileName, "localhost", NetworkHandlerIF.networkDataServerPort );
//
//            // start data server that python program will use to generate link summaries.
//            nh_new.startDataServer();
//
//            // run python in an external dos command to generate link category summary reports file
//            DosCommand.runDOSCommand ( winCmdLocation, commandString );
//
//            // stop the data server.
//            nh_new.stopDataServer();
//
//        }
//
//    }

    public void runLinkSummaries(NetworkHandlerIF nh) {
        String linkSummaryFileName = null;

        // get output filename for link summary statustics report written by python program
        linkSummaryFileName = appRb.getString( "linkSummary.fileName" );
        if ( linkSummaryFileName != null ) {
            nh.startDataServer();

            String assignmentPeriod = nh.getTimePeriod();

            int index = linkSummaryFileName.indexOf(".");
            if ( index < 0 ) {
                linkSummaryFileName += "_" + assignmentPeriod;
            }
            else {
                String extension = linkSummaryFileName.substring(index);
                linkSummaryFileName = linkSummaryFileName.substring(0, index);
                linkSummaryFileName += "_" + assignmentPeriod + extension;
            }

            String pythonSrc = appRb.getString( PYTHON_PROGRAM_TARGET );
            String pythonExecutable = ResourceUtil.getProperty(appRb, "python.executable");
            String a2bFileName = globalRb.getString( "alpha2beta.file" );
            String countsFileName = appRb.getString( "counts.file" );

            ProcessBuilder pb = new ProcessBuilder(
                    pythonExecutable,
                    pythonSrc,
                    "" + COUNT_YEAR,
                    assignmentPeriod,
                    linkSummaryFileName,
                    a2bFileName,
                    countsFileName,
                    "localhost",
                    "" + NetworkHandlerIF.networkDataServerPort);


            pb.redirectErrorStream(true);
            final Process p;
            try {
                p = pb.start();
                //log error stream
                new Thread(new Runnable() {
                    public void run() {
                        logInputStream(p.getErrorStream(),true);
                    }
                }).start();
                logInputStream(p.getInputStream(),false);
                if (p.waitFor() != 0)
                    logger.error("An error occurred while trying to run TS python link comparison");
            } catch (IOException e) {
                logger.error("An IO exception occurred while trying to run TS python link comparison",e);
            } catch (InterruptedException e) {
                logger.error("Interrupted exception caught waiting for TS python link comparison to finish",e);
            } finally {
                nh.stopDataServer();
            }
        }
    }

    private void logInputStream(InputStream stream, boolean error) {
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
        try {
            String line;
            while ((line = reader.readLine()) != null)
                if (error)
                    logger.error(line);
                else
                    logger.info(line);
        } catch (IOException e) {
            logger.error("An IO exception occurred while logging TS python link comparison output",e);
        } finally {
            try {
                reader.close();
            } catch (IOException e) {
                //swallow
            }
        }
    }
        
}
