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
package com.pb.tlumip.ts.assign;

/**
 *
 * @author    Jim Hicks
 * @version   1.0, 9/30/2004
 * 
 * This model implements an aggregate equilibrium traffic assignment procedure
 * and is intended for use with the ODOT R-based travel demand model package.
 * The traffic assignment class written in java is to be executed by an R program. 
 */


import com.pb.common.datafile.OLD_CSVFileReader;
import com.pb.common.datafile.TableDataSet;
import com.pb.common.util.ResourceUtil;
import com.pb.tlumip.model.ModeType;
import com.pb.tlumip.ts.NetworkHandler;
import com.pb.tlumip.ts.NetworkHandlerIF;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.util.Date;
import java.util.HashMap;


public class SimpleAssign {

	protected static Logger logger = Logger.getLogger("com.pb.tlumip.ts.odot");

    final char[] highwayModeCharacters = { 'a', 'd', 'e', 'f', 'g', 'h' };

    //protected static NetworkHandlerIF nh = null;

    HashMap tsPropertyMap = null;
    HashMap globalPropertyMap = null;
    
	
	public SimpleAssign() {
	}
    
	
	
    public static void main (String[] args) {
        
        SimpleAssign a = new SimpleAssign();
        System.out.println ( a.assignAggregateTrips( args[0], args[1] ) );
		
    }

    
    
    public String assignAggregateTrips ( String tsPropertyFileName, String globalPropertyFileName ) {
        
		long startTime = System.currentTimeMillis();
		
		String period = "peak";
		
		double[][][] multiclassTripTable = null;
		
		String loggerMessage=null;
		
		String myDateString;

		// create a HashMap of ts properties values
        tsPropertyMap = ResourceUtil.getResourceBundleAsHashMap( tsPropertyFileName );

        // create a HashMap of global properties values
        globalPropertyMap = ResourceUtil.getResourceBundleAsHashMap( globalPropertyFileName );


		
		myDateString = DateFormat.getDateTimeInstance().format(new Date());
		logger.info ("creating Highway Network object at: " + myDateString);
        NetworkHandlerIF nh = NetworkHandler.getInstance();
        setupNetwork( nh, tsPropertyMap, globalPropertyMap, period );
        logger.info ("done building Network object.");
		
		
		// create Frank-Wolfe Algortihm Object
		myDateString = DateFormat.getDateTimeInstance().format(new Date());
		logger.info ("creating FW object at: " + myDateString);
		FW fw = new FW();
		fw.initialize( tsPropertyMap, nh );

		// read PT trip list into o/d trip matrix
		myDateString = DateFormat.getDateTimeInstance().format(new Date());
		logger.info ("reading trip lists at: " + myDateString);
		multiclassTripTable = createMulticlassDemandMatrices( period, nh.getNumUserClasses(), nh.getNumCentroids(), nh.getNodeIndex(), nh.getAssignmentGroupChars(), nh.userClassesIncludeTruck() );

		//Compute Frank-Wolfe solution
		myDateString = DateFormat.getDateTimeInstance().format(new Date());
		logger.info ("starting fw at: " + myDateString);
		fw.iterate ( multiclassTripTable );
		myDateString = DateFormat.getDateTimeInstance().format(new Date());
		logger.info ("done with fw at: " + myDateString);


        loggerMessage = "assignAggregateTrips() finished in " + ( System.currentTimeMillis() - startTime ) / 60000.0  + " minutes";
		logger.info( loggerMessage );
		
		return loggerMessage;

    }
    
    
    private void setupNetwork ( NetworkHandlerIF nh, HashMap appMap, HashMap globalMap, String timePeriod ) {
        
        String networkFileName = (String)appMap.get("d211.fileName");
        String networkDiskObjectFileName = (String)appMap.get("NetworkDiskObject.file");
        
        String turnTableFileName = (String)appMap.get( "d231.fileName" );
        String networkModsFileName = (String)appMap.get( "d211Mods.fileName" );
        
        String vdfFileName = (String)appMap.get("vdf.fileName");
        String vdfIntegralFileName = (String)appMap.get("vdfIntegral.fileName");
        
        String a2bFileName = (String) globalMap.get( "alpha2beta.file" );
        
        // get peak or off-peak volume factor from properties file
        String volumeFactor="";
        if ( timePeriod.equalsIgnoreCase( "peak" ) )
            volumeFactor = (String)globalMap.get("am.peak.volume.factor");
        else if ( timePeriod.equalsIgnoreCase( "offpeak" ) )
            volumeFactor = (String)globalMap.get("offpeak.volume.factor");
        else {
            logger.error ( "time period specifed as: " + timePeriod + ", but must be either 'peak' or 'offpeak'." );
            System.exit(-1);
        }
        
        String userClassesString = (String)appMap.get("userClass.modes");
        String truckClass1String = (String)appMap.get( "truckClass1.modes" );
        String truckClass2String = (String)appMap.get( "truckClass2.modes" );
        String truckClass3String = (String)appMap.get( "truckClass3.modes" );
        String truckClass4String = (String)appMap.get( "truckClass4.modes" );
        String truckClass5String = (String)appMap.get( "truckClass5.modes" );

        String walkSpeed = (String)globalMap.get( "sdt.walk.mph" );
        
        
        String[] propertyValues = new String[NetworkHandler.NUMBER_OF_PROPERTY_VALUES];
        
        propertyValues[NetworkHandlerIF.NETWORK_FILENAME_INDEX] = networkFileName;
        propertyValues[NetworkHandlerIF.NETWORK_DISKOBJECT_FILENAME_INDEX] = networkDiskObjectFileName;
        propertyValues[NetworkHandlerIF.VDF_FILENAME_INDEX] = vdfFileName;
        propertyValues[NetworkHandlerIF.VDF_INTEGRAL_FILENAME_INDEX] = vdfIntegralFileName;
        propertyValues[NetworkHandlerIF.ALPHA2BETA_FILENAME_INDEX] = a2bFileName;
        propertyValues[NetworkHandlerIF.TURNTABLE_FILENAME_INDEX] = turnTableFileName;
        propertyValues[NetworkHandlerIF.NETWORKMODS_FILENAME_INDEX] = networkModsFileName;
        propertyValues[NetworkHandlerIF.VOLUME_FACTOR_INDEX] = volumeFactor;
        propertyValues[NetworkHandlerIF.USER_CLASSES_STRING_INDEX] = userClassesString;
        propertyValues[NetworkHandlerIF.TRUCKCLASS1_STRING_INDEX] = truckClass1String;
        propertyValues[NetworkHandlerIF.TRUCKCLASS2_STRING_INDEX] = truckClass2String;
        propertyValues[NetworkHandlerIF.TRUCKCLASS3_STRING_INDEX] = truckClass3String;
        propertyValues[NetworkHandlerIF.TRUCKCLASS4_STRING_INDEX] = truckClass4String;
        propertyValues[NetworkHandlerIF.TRUCKCLASS5_STRING_INDEX] = truckClass5String;
        propertyValues[NetworkHandlerIF.WALK_SPEED_INDEX] = walkSpeed;
        
        nh.setupHighwayNetworkObject ( timePeriod, propertyValues );
        
    }
    
    private double[][][] createMulticlassDemandMatrices (String timePeriod, int numUserClasses, int numCentroids, int[] nodeIndex, char[][] assignmentGroupChars, boolean userClassesIncludeTruck) {
        
        String myDateString;
        
        double[][][] multiclassTripTable = new double[numUserClasses][][];
        

        int startHour = 0;
        int endHour = 0;

        // get trip list filenames from property file
        String ptFileName = (String)tsPropertyMap.get("pt.fileName");
        String ctFileName = (String)tsPropertyMap.get("ct.fileName");

        

        if ( timePeriod.equalsIgnoreCase( "peak" ) ) {
            // get peak period definitions from property files
            startHour = Integer.parseInt( (String)globalPropertyMap.get("am.peak.start") );
            endHour = Integer.parseInt( (String)globalPropertyMap.get("am.peak.end") );
        }
        else if ( timePeriod.equalsIgnoreCase( "offpeak" ) ) {
            // get off-peak period definitions from property files
            startHour = Integer.parseInt( (String)globalPropertyMap.get("offpeak.start") );
            endHour = Integer.parseInt( (String)globalPropertyMap.get("offpeak.end") );
        }


        // check that at least one valid user class has been defined
        if ( numUserClasses == 0 ) {
            logger.error ( "No valid user classes defined in the application properties file.", new RuntimeException() );
        }
        
        
        
        // read PT trip list into o/d trip matrix if auto user class was defined
        boolean assignmentGroupContainsAuto = false;
        for (int i=0; i < assignmentGroupChars[0].length; i++) {
            if ( assignmentGroupChars[0][i] == 'a' ) {
                assignmentGroupContainsAuto = true;
                break;
            }
        }
                
        if ( assignmentGroupContainsAuto ) {
            myDateString = DateFormat.getDateTimeInstance().format(new Date());
            logger.info ("reading " + timePeriod + " PT trip list at: " + myDateString);
            multiclassTripTable[0] = getAutoTripTableFromPTList ( ptFileName, startHour, endHour, numCentroids, nodeIndex );
        }
        else {
            logger.info ("no auto class defined, so " + timePeriod + " PT trip list was not read." );
        }

        
        // read CT trip list into o/d trip matrix if at least one truck class was defined
        if ( userClassesIncludeTruck ) {
            myDateString = DateFormat.getDateTimeInstance().format(new Date());
            logger.info ("reading " + timePeriod + " CT trip list at: " + myDateString);
            double[][][] truckTripTables = getTruckAssignmentGroupTripTableFromCTList ( ctFileName, startHour, endHour, numUserClasses, numCentroids, nodeIndex, assignmentGroupChars );

            for(int i=0; i < truckTripTables.length - 1; i++)
                multiclassTripTable[i+1] = truckTripTables[i];
        }


        return multiclassTripTable;
        
    }


    
    private double[][] getAutoTripTableFromPTList ( String fileName, int startPeriod, int endPeriod, int numCentroids, int[] nodeIndex ) {
        
        int orig;
        int dest;
        int startTime;
        int mode;
        int o;
        int d;
        int allAutoTripCount=0;
        int tripCount=0;
        
        
        double[][] tripTable = new double[numCentroids+1][numCentroids+1];

        
        // read the PT output person trip list file into a TableDataSet
        OLD_CSVFileReader reader = new OLD_CSVFileReader();

        String[] columnsToRead = { "origin", "destination", "tripStartTime", "tripMode" };
        TableDataSet table = null;
        try {
            if ( fileName != null) {

                table = reader.readFile(new File( fileName ), columnsToRead);

                // traverse the trip list in the TableDataSet and aggregate trips to an o/d trip table
                for (int i=0; i < table.getRowCount(); i++) {
                    
                    orig = (int)table.getValueAt( i+1, "origin" );
                    dest = (int)table.getValueAt( i+1, "destination" );
                    startTime = (int)table.getValueAt( i+1, "tripStartTime" );
                    mode = (int)table.getValueAt( i+1, "tripMode" );
                    
                    o = nodeIndex[orig];
                    d = nodeIndex[dest];
                    
                    // accumulate all peak period highway mode trips
                    if ( (mode == ModeType.AUTODRIVER || mode == ModeType.AUTOPASSENGER) ) {
                        
                        if ( (startTime >= startPeriod && startTime <= endPeriod) ) {
    
                            tripTable[o][d]++;
                            tripCount++;
                        
                        }

                        allAutoTripCount++;
                    }
                    
                }
                
                // done with trip list TabelDataSet
                table = null;

            }
            
        } catch (IOException e) {
            logger.error ( "", e );
        }


        logger.info (allAutoTripCount + " total auto network trips read from PT entire file");
        logger.info (tripCount + " total auto network trips read from PT file for period " + startPeriod +
                " to " + endPeriod);

        return tripTable;
            
    }
    

    private double[][][] getTruckAssignmentGroupTripTableFromCTList ( String fileName, int startPeriod, int endPeriod, int numUserClasses, int numCentroids, int[] nodeIndex, char[][] assignmentGroupChars ) {

        int orig;
        int dest;
        int startTime;
        int mode;
        int o;
        int d;
        int group;
        char modeChar;
        String truckType;
        double tripFactor = 1.0;
        int allTruckTripCount=0;
        

        double[] tripsByUserClass = new double[highwayModeCharacters.length];
        double[] tripsByAssignmentGroup = new double[numUserClasses];
        double[][][] tripTable = new double[numUserClasses][numCentroids+1][numCentroids+1];



        // read the CT output file into a TableDataSet
        OLD_CSVFileReader reader = new OLD_CSVFileReader();

        TableDataSet table = null;
        int tripRecord = 0;
        try {
            if ( fileName != null ) {

                table = reader.readFile(new File( fileName ));

                // traverse the trip list in the TableDataSet and aggregate trips to an o/d trip table
                for (int i=0; i < table.getRowCount(); i++) {
                    tripRecord = i+1;
                    
                    orig = (int)table.getValueAt( i+1, "origin" );
                    dest = (int)table.getValueAt( i+1, "destination" );
                    startTime = (int)table.getValueAt( i+1, "tripStartTime" );
                    truckType = (String)table.getStringValueAt( i+1, "truckType" );
                    tripFactor = (int)table.getValueAt( i+1, "tripFactor" );
    
                    mode = Integer.parseInt( truckType.substring(3) );
                    modeChar = highwayModeCharacters[mode];
                    group = -1;
                    for (int j=1; j < assignmentGroupChars.length; j++) {
                        for (int k=0; k < assignmentGroupChars[j].length; k++) {
                            if ( assignmentGroupChars[j][k] == modeChar ) {
                                group = j;
                                break;
                            }
                        }
                    }
                    if ( group < 0 ) {
                        logger.error ( "modeChar = " + modeChar + " associated with CT integer mode = " + mode + " not found in any asignment group." );
                        System.exit(-1);
                    }
                    
                    o = nodeIndex[orig];
                    d = nodeIndex[dest];
    
                    // accumulate all peak period highway mode trips
                    if ( startTime >= startPeriod && startTime <= endPeriod ) {
    
                        tripTable[group-1][o][d] += tripFactor;
                        tripsByUserClass[mode-1] += tripFactor;
                        tripsByAssignmentGroup[group-1] += tripFactor;
    
                    }
                    
                    allTruckTripCount += tripFactor;
    
                }
    
                // done with trip list TabelDataSet
                table = null;

            }
            
        } catch (Exception e) {
            logger.error ("exception caught reading CT truck trip record " + tripRecord, e);
            System.exit(-1);
        }

        
        logger.info (allTruckTripCount + " total trips by all truck user classes read from CT file.");
        logger.info ("trips by truck user class read from CT file from " + startPeriod + " to " + endPeriod + ":");
        for (int i=0; i < tripsByUserClass.length; i++)
            if (tripsByUserClass[i] > 0)
                logger.info ( tripsByUserClass[i] + " truck trips with user class " + highwayModeCharacters[i+1] );

        logger.info ("trips by truck assignment groups read from CT file from " + startPeriod + " to " + endPeriod + ":");
        for (int i=0; i < tripsByAssignmentGroup.length; i++)
            if (tripsByAssignmentGroup[i] > 0)
                logger.info ( tripsByAssignmentGroup[i] + " truck trips in assignment group " + (i+1) );

        return tripTable;

    }
    
}
