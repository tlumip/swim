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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.text.DateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;

import com.pb.common.calculator.LinkCalculator;
import com.pb.common.calculator.LinkFunction;
import com.pb.common.datafile.D211FileReader;
import com.pb.common.datafile.D231FileReader;
import com.pb.common.datafile.OLD_CSVFileReader;
import com.pb.common.datafile.TableDataSet;
import com.pb.common.util.IndexSort;
import com.pb.common.matrix.AlphaToBeta;
import com.pb.tlumip.ts.NetworkHandlerIF;
import com.pb.tlumip.ts.transit.AuxTrNet;

/**
 * This Network class contains the link and node data tables and
 * index arrays for representing the network in forward star notation.
 *
 */

public class Network implements Serializable {

    static final  int NOT_USED_FLAG = 99999999;
    
    
    static final String OUTPUT_FLOW_FIELDS_START_WITH = "assignmentFlow";
    static final String OUTPUT_COST_FIELDS_START_WITH = "linkCost";
    static final String OUTPUT_TIME_FIELD = "assignmentTime";
    static final String OUTPUT_ANODE_FIELD = "anode";
    static final String OUTPUT_BNODE_FIELD = "bnode";
    static final String OUTPUT_CAPACITY_FIELD = "capacity";
	
    final char[] highwayModeCharacters = { 'a', 'd', 'e', 'f', 'g', 'h' };
    final char[] transitModeCharacters = { 'b', 'i', 'k', 'l', 'm', 'p', 'r', 's', 't', 'w', 'x' };
    final char[] hsrModeCharacters = { 't' };
    final char[] airModeCharacters = { 'n' };
    
    
    
	protected static transient Logger logger = Logger.getLogger(Network.class);

	
	HashMap userClassMap = new HashMap();
	HashMap assignmentGroupMap = new HashMap();
    char[][] assignmentGroupChars = null;

	int minCentroidLabel;
	int maxCentroidLabel;
	
	String assignmentPeriod;

    float volumeFactor;

    double WALK_SPEED;

	
	int maxCentroid;
	int numCentroids;
	int maxNode;
    int numLinks;
    int numUserClasses;

    char[] userClasses = null;
    float[] userClassPces = null;

    float[] userClassVotPk;
    float[] userClassVotOp;
    float[] userClassOpCost;
    
    double[] volau;
    double[] totAonFlow;

    String[] linkLabels = null;
    boolean[] validLinks = null;
    boolean[][] validLinksForClass = null;
    int[][] onewayLinksForClass = null;
    int[] indexNode = null;
    int[] nodeIndex = null;
    int[] internalNodeToNodeTableRow = null;
    int[] sortedLinkIndexA;
    int[] sortedLinkIndexB;
    int[] ipa;
    int[] ipb;
	int[] ia;
	int[] ib;
    int[] uniqueIds;
    int[] drops;

	TableDataSet nodeTable = null;
	TableDataSet linkTable = null;
	TableDataSet linkModsTable = null;
	TableDataSet derivedLinkTable = null;

	LinkFunction lf = null;
	LinkFunction lfi = null;
	LinkCalculator fdLc = null;
	LinkCalculator fpLc = null;
	LinkCalculator fdiLc = null;
	LinkCalculator fpiLc = null;
	LinkCalculator ftLc = null;

    int[][] turnPenaltyIndices = null;
    float[][] turnPenaltyArray = null;
	float[][][] turnTable = null;

    int[] externalZoneLabels = null;

    int[] alphaDistrictIndex = null;
    String[] districtNames = null;


    public Network ( String period, String[] propertyValues ) { 
            
        assignmentPeriod = period;
        volumeFactor = Float.parseFloat( propertyValues[NetworkHandlerIF.VOLUME_FACTOR_INDEX] );

        String d211File = propertyValues[NetworkHandlerIF.NETWORK_FILENAME_INDEX];
        String d211ModsFile = propertyValues[NetworkHandlerIF.NETWORKMODS_FILENAME_INDEX];
        String d231File = propertyValues[NetworkHandlerIF.TURNTABLE_FILENAME_INDEX];
        String extraAttribsFile = propertyValues[NetworkHandlerIF.EXTRA_ATTRIBS_FILENAME_INDEX];
        

        String zoneIndexFile = propertyValues[NetworkHandlerIF.ALPHA2BETA_FILENAME_INDEX];
        maxCentroidLabel = getMaxCentroid( zoneIndexFile );

        minCentroidLabel = 1;


//  previous version of code defined costs in cents and VOT and OP_COST by user class were hard coded in Network class        
//      static final float[][] VOT  = { { 1150, 1150, 1150, 1150, 1150 }, {  770,  770,  770,  770,  770 } };   // vehicle class VOT: peak $11.50/hr, offpeak $7.70/hr
//      static final float[] OP_COST = {   12,   12,   12,   12,   12 };   // vehicle class cents/mile
        
        // get value of time and operating cost values (in dollars) by user classes to convert link dollar costs to units of minutes.
        userClassVotPk = getUserClassFloatValuesFromProperties( propertyValues[NetworkHandlerIF.USER_CLASS_VOT_PK_STRING_INDEX] );
        userClassVotOp = getUserClassFloatValuesFromProperties( propertyValues[NetworkHandlerIF.USER_CLASS_VOT_OP_STRING_INDEX] );
        userClassOpCost = getUserClassFloatValuesFromProperties( propertyValues[NetworkHandlerIF.USER_CLASS_OP_COST_STRING_INDEX] );
        
        
        // get user classes to assign and assignment groups (which classes are combined together for assigning)
        userClasses = getUserClassesFromProperties ( propertyValues[NetworkHandlerIF.USER_CLASSES_STRING_INDEX] );
        userClassPces = getUserClassPcesFromProperties ( propertyValues[NetworkHandlerIF.USER_CLASS_PCES_STRING_INDEX] );
        numUserClasses = userClasses.length;

        // check for valid PCE values
        if ( userClassPces.length != numUserClasses ) {
            String errorString = String.format("%d user classes were specified for %s, but %d user class PCEs were specified.", this.numUserClasses, propertyValues[NetworkHandlerIF.USER_CLASSES_STRING_INDEX], userClassPces.length);
            errorString += "\n" + "the number of user classes and number of PCE values must be equal.";
            throw new RuntimeException( errorString );
        }
        for (int m=0; m < numUserClasses; m++) {
            if ( userClassPces[m] <= 0.0 ) {
                String errorString = String.format("PCE for user class %d (%c) was specified as %f, but must by > 0.0.", m, userClasses[m], userClassPces[m]);
                throw new RuntimeException( errorString );
            }
        }


        String[] truckClassPropertyValues = new String[6];
        truckClassPropertyValues[1] = propertyValues[NetworkHandlerIF.TRUCKCLASS1_STRING_INDEX];
        truckClassPropertyValues[2] = propertyValues[NetworkHandlerIF.TRUCKCLASS2_STRING_INDEX];
        truckClassPropertyValues[3] = propertyValues[NetworkHandlerIF.TRUCKCLASS3_STRING_INDEX];
        truckClassPropertyValues[4] = propertyValues[NetworkHandlerIF.TRUCKCLASS4_STRING_INDEX];
        truckClassPropertyValues[5] = propertyValues[NetworkHandlerIF.TRUCKCLASS5_STRING_INDEX];
        
        assignmentGroupChars = setAssignmentGroups( truckClassPropertyValues );
        
        
        logger.info ( "Mode codes for user classes in multiclass assignment:" );
        for (int i=0; i < userClasses.length; i++)
            logger.info ( "     " + i + ": " + userClasses[i] );
        

        if ( propertyValues[NetworkHandlerIF.WALK_SPEED_INDEX] != null && ! propertyValues[NetworkHandlerIF.WALK_SPEED_INDEX].equals("") )
            this.WALK_SPEED = Double.parseDouble ( propertyValues[NetworkHandlerIF.WALK_SPEED_INDEX] );
        else
            this.WALK_SPEED = 3.0;
		

		// read the node and link tables
        float[][] turnDefs = null;

		D211FileReader d211 = new D211FileReader();
		D231FileReader d231 = new D231FileReader();
		try {
			nodeTable = d211.readNodeTable( new File(d211File) );
			linkTable = d211.readLinkTable( new File(d211File) );
			
			if ( d211ModsFile != null && ! d211ModsFile.equals("") )
			    linkModsTable = d211.readLinkTableMods( new File(d211ModsFile) );

			if ( d231File != null && ! d231File.equals("") )
			    turnDefs = d231.readTurnTable( new File(d231File) );
		
		}
		catch (Exception e) {
			logger.error ( "", e );
		}

		// read link function definitions files (delay functions and integrals functions)		
		lf = new LinkFunction ( propertyValues[NetworkHandlerIF.VDF_FILENAME_INDEX], "vdf");
		lfi = new LinkFunction ( propertyValues[NetworkHandlerIF.VDF_INTEGRAL_FILENAME_INDEX], "vdf");
		
		
		// set the internal numbering for nodes and their correspondence to external numbering.
		setInternalNodeNumbering ();

		// define the forward star index arrays, first by anode then by bnode
		ia = linkTable.getColumnAsInt( "ia" );
		ib = linkTable.getColumnAsInt( "ib" );
        
        sortedLinkIndexA = IndexSort.indexSort( ia );
        ipa = setForwardStarArrays ( ia, sortedLinkIndexA );
        
        sortedLinkIndexB = IndexSort.indexSort( ib );
        ipb = setForwardStarArrays ( ib, sortedLinkIndexB );
        
        
		// calculate the derived link attributes for the network
		derivedLinkTable = deriveLinkAttributes();

		// merge the derived link attributes into the linkTable TableDataSet,
		// then we're done with the derived table.
		linkTable.merge ( derivedLinkTable );
		derivedLinkTable = null;
		

        
        // update linktable with extra attributes
        readLinkAttributesCsvFile ( extraAttribsFile );
        
        
        numLinks = linkTable.getRowCount();


        // calculate the congested link travel times based on the vdf functions defined
		fdLc = new LinkCalculator ( linkTable, lf.getFunctionStrings( "fd" ), "vdf" );
		applyVdfs();
		logLinkTimeFreqs();
		


		// add turn defintions and turn penalty indices into linkTable
		if (turnDefs != null) {
			fpLc = new LinkCalculator ( linkTable, lf.getFunctionStrings( "fp" ), "turnIndex" );
			setTurnPenalties( turnDefs );
		}
        else {
            turnTable = new float[0][0][0];
            turnPenaltyIndices = new int[0][0];
            turnPenaltyArray = new float[0][0];
        }


		// define link calculators for use in computing objective function and lambda vales
		fdiLc = new LinkCalculator ( linkTable, lfi.getFunctionStrings( "fd" ), "vdf" );
		fpiLc = new LinkCalculator ( linkTable, lfi.getFunctionStrings( "fp" ), "turnIndex" );

		ftLc = new LinkCalculator ( linkTable, lf.getFunctionStrings( "ft" ), "vdf" );
	
        int[] externalNodes = getNodes();
        internalNodeToNodeTableRow = new int[externalNodes.length];
        for (int i=0; i < externalNodes.length; i++) {
            int extNode = externalNodes[i];
            int intNode = nodeIndex[extNode];
            
            // there may be nodes in the node table that are not in the link table
            if ( intNode >= 0 )
                internalNodeToNodeTableRow[intNode] = i;
        }
        
        
        // set the link oneway attribute values
        setOnewayLinks();
        
        
        setDroppedLinks();
        
        createAlphaDistrictArray( zoneIndexFile );
        
    }

    
	public String getTimePeriod () {
		return assignmentPeriod;
	}

	public int getMaxCentroid () {
		return maxCentroid;
	}

	public int getNumCentroids () {
		return numCentroids;
	}

    public int getLinkCount () {
        return numLinks;
    }

    public int getNodeCount () {
        return nodeTable.getRowCount();
    }

	public double getWalkTime (float dist) {
		return(60.0*dist/WALK_SPEED);
	}

	public boolean isCentroid ( int node ) {
		return ( node >= minCentroidLabel && node <= maxCentroidLabel );
	}
	
	
	public int[] getIa () {
		return linkTable.getColumnAsInt( "ia" );
	}

	public int[] getIb () {
		return linkTable.getColumnAsInt( "ib" );
	}

    public int[] getIpa () {
        return ipa;
    }

    public int[] getIpb () {
        return ipb;
    }

    public int[] getSortedLinkIndexA () {
        return sortedLinkIndexA;
    }

    public int[] getSortedLinkIndexB () {
        return sortedLinkIndexB;
    }

    public int[] getIndexNode () {
        return indexNode;
    }

    public int getExternalNode (int internalNode) {
        return indexNode[internalNode];
    }

    public int getInternalNode (int externalNode) {
        return nodeIndex[externalNode];
    }

    public int[] getNodeIndex () {
        return nodeIndex;
    }

    public int[] getVdfIndex () {
        return linkTable.getColumnAsInt( "vdf" );
    }

	public boolean[] getCentroid () {
		return linkTable.getColumnAsBoolean( "centroid" );
	}

	public double[] getCapacity () {
		return linkTable.getColumnAsDouble( "capacity" );
	}

    public double[] getOriginalCapacity () {
        return linkTable.getColumnAsDouble( "originalCapacity" );
    }

    public double[] getTotalCapacity () {
        return linkTable.getColumnAsDouble( "totalCapacity" );
    }

    public double[] getCongestedTime () {
        return linkTable.getColumnAsDouble( "congestedTime" );
    }

    public double[] getLinkGeneralizedCost () {
        return linkTable.getColumnAsDouble( "generalizedCost" );
    }

	public double[] getTransitTime () {
		return linkTable.getColumnAsDouble( "transitTime" );
	}

    public double getSumOfVdfIntegrals () {

        double[] integrals = linkTable.getColumnAsDouble("vdfIntegral");
        
        double sum = 0.0;
        for (int k=0; k < integrals.length; k++)
            if ( validLinks[k] )
                sum += integrals[k];
            
        return sum;
    }

    public double[] getFreeFlowTime () {
        return linkTable.getColumnAsDouble( "freeFlowTime" );
    }

    public double[] getFreeFlowSpeed () {
        return linkTable.getColumnAsDouble( "freeFlowSpeed" );
    }

	public double[] getDist () {
		return linkTable.getColumnAsDouble( "dist" );
	}

    public int[] getLinkType () {
        return linkTable.getColumnAsInt( "type" );
    }

    public int[] getTaz () {
        return linkTable.getColumnAsInt( "taz" );
    }

    public int[] getDrops () {
        return linkTable.getColumnAsInt( "drops" );
    }

    public int[] getUniqueIds () {
        return linkTable.getColumnAsInt( "uniqueIds" );
    }

    public float[] getUserClassPces() {
        return userClassPces;
    }

    public double[] getLanes () {
        return linkTable.getColumnAsDouble( "lanes" );
    }

	public String[] getMode () {
		return linkTable.getColumnAsString( "mode" );
	}

	public char[] getUserClasses () {
		return userClasses;
	}

	public int getNumUserClasses () {
		return numUserClasses;
	}
	
	public boolean userClassesIncludeAuto () {
		// return true if the auto class is included in the list of user classes to assign
		
		boolean autoIncluded = false;
		for (int i=0; i < userClasses.length; i++) {
			if ( userClasses[i] == 'a' ) {
				autoIncluded = true;
				break;
			}
		}
			
		return autoIncluded;
	}

	public boolean userClassesIncludeTruck () {
		// return true if at least one truck class is included in the list of user classes to assign

		boolean truckIncluded = false;
		for (int i=0; i < userClasses.length; i++) {
			if ( userClasses[i] == 'd' || userClasses[i] == 'e' || userClasses[i] == 'f' || userClasses[i] == 'g' || userClasses[i] == 'h' ) {
				truckIncluded = true;
				break;
			}
		}
			
		return truckIncluded;
	}

	public double getWalkSpeed () {
		return WALK_SPEED;
	}

    public double[][] getFlows () {

        double[][] flows = new double[userClasses.length][];

        for (char c : userClasses) {
            int m = getUserClassIndex(c);
            flows[m] = linkTable.getColumnAsDouble( "flow_" + c );
        }

        return flows;
    }

    public double[] getTotalLinkCost () {
        return linkTable.getColumnAsDouble( "totalLinkCost" );
    }

    public double[] getLinkAttribCosts ( char c ) {
        return linkTable.getColumnAsDouble( String.format("linkAttribCosts_%c", c) );
    }

    public double[] getVolau() {
        return linkTable.getColumnAsDouble( "volau" );
    }
    
    public double[] getVolad() {
        return linkTable.getColumnAsDouble( "volad" );
    }
    
    public int[] getNodes () {
        return nodeTable.getColumnAsInt( "node" );
    }
    
    public double[] getNodeX () {
        return nodeTable.getColumnAsDouble( "x" );
    }
    
	public double[] getNodeY () {
		return nodeTable.getColumnAsDouble( "y" );
	}
    	
    public int[] getInternalNodeToNodeTableRow () {
        return internalNodeToNodeTableRow;
    }
    
    public double[] getCoordsForLink(int k) {
        int r = internalNodeToNodeTableRow[ia[k]];
        int s = internalNodeToNodeTableRow[ib[k]];
        double[] nodeX = getNodeX();
        double[] nodeY = getNodeY();
        double[] returnValue = { nodeX[r], nodeY[r], nodeX[s], nodeY[s] };
        return returnValue;
    }
    
	public char[][] getAssignmentGroupChars () {
        return assignmentGroupChars;
	}
	
	public int getUserClassIndex ( char modeChar ) {
		return ((Integer)userClassMap.get( String.valueOf(modeChar) )).intValue();
	}
    
    public int[][] getTurnPenaltyIndices () {
        return turnPenaltyIndices;
    }
    
    public float[][] getTurnPenaltyArray () {
        return turnPenaltyArray;
    }
    
    public char[] getHighwayModeCharacters() {
        return highwayModeCharacters;
    }
    
    public char[] getTransitModeCharacters() {
        return transitModeCharacters;
    }
    
    public boolean[] getValidLinksForTransitPaths () {
        boolean[] validLinks = new boolean[numLinks];
        String[] linkModes = linkTable.getColumnAsString( "mode" );
        
        for (int k=0; k < numLinks; k++) {
            for (int i=0; i < transitModeCharacters.length; i++) {
                if ( linkModes[k].indexOf( transitModeCharacters[i] ) >= 0 )
                    validLinks[k] = true;
            }
        }
        
        return validLinks;
    }

    public boolean[][] getValidLinksForAllClasses () {
        return validLinksForClass;
    }

    public boolean[] getValidLinksForClass ( int userClass ) {
        return validLinksForClass[userClass];
    }

    public boolean[] getValidLinksForClassChar ( char modeChar ) {
		int userClassIndex = getUserClassIndex(modeChar);
        return validLinksForClass[userClassIndex];
    }

    public int[] getOnewayLinksForClass ( int userClass ) {
        return onewayLinksForClass[userClass];
    }

    public String getAssignmentResultsString () {
        return OUTPUT_FLOW_FIELDS_START_WITH;
    }
    
    public String getAssignmentResultsAnodeString () {
        return OUTPUT_ANODE_FIELD;
    }
    
    public String getAssignmentResultsBnodeString () {
        return OUTPUT_BNODE_FIELD;
    }
    
    public String getAssignmentResultsTimeString () {
        return OUTPUT_TIME_FIELD;
    }
    
    public double[] setLinkGeneralizedCost () {

        double[] ctime = getCongestedTime();
        double[] totalLinkCost = getTotalLinkCost();

        double[] gc = new double[ctime.length];


        // loop over links and add link cost to congested time
        for (int i=0; i < ctime.length; i++)
            gc[i] = ctime[i] + totalLinkCost[i];


        linkTable.setColumnAsDouble( linkTable.getColumnPosition("generalizedCost"), gc );
		
		return gc;
    }

    public void setVolau ( double[] volau ) {
        this.volau = volau;
        linkTable.setColumnAsDouble( linkTable.getColumnPosition("volau"), volau );
    }

    public void setTimau ( double[] timau ) {
        linkTable.setColumnAsDouble( linkTable.getColumnPosition("congestedTime"), timau );
    }

    public void setTaz ( int[] taz ) {
        linkTable.appendColumn( taz, "taz" );
    }

    public void setDrops ( int[] drops ) {
        this.drops = drops;
        linkTable.appendColumn( drops, "drops" );
    }

    public void setLinkLabels ( String[] labels ) {
        linkLabels = labels;
        linkTable.appendColumn( labels, "label" );
    }

    public void setRevisedModes ( String[] revMode ) {
        int column = linkTable.getColumnPosition("mode");
        for (int i=0; i < linkTable.getRowCount(); i++) {
            linkTable.setStringValueAt( i+1, column, revMode[i] );
       }
    }

    public void setTotalLinkCost( double[] cost){
        linkTable.appendColumn ( cost, "totalLinkCost" );
    }

    public void setLinkAttribCosts( double[][] cost){
        for ( char c : userClasses ) {
            int m = getUserClassIndex(c);
            linkTable.appendColumn ( cost[m], String.format("linkAttribCosts_%c", c) );
        }
    }

    public void setUniqueIds ( int[] ids ) {
        uniqueIds = ids;
        linkTable.appendColumn( ids, "uniqueIds" );
    }

    public void setOriginalCapacity ( double[] originalCapacity ) {
        linkTable.setColumnAsDouble( linkTable.getColumnPosition("originalCapacity"), originalCapacity );
    }

    public void setCapacity ( double[] capacity ) {
        linkTable.setColumnAsDouble( linkTable.getColumnPosition("capacity"), capacity );
    }

    public void setTotalCapacity ( double[] totalCapacity ) {
        linkTable.setColumnAsDouble( linkTable.getColumnPosition("totalCapacity"), totalCapacity );
    }

    public void setVolCapRatios () {
		
    	double[] totalVolCapRatio = linkTable.getColumnAsDouble( "totalVolCapRatio" );
    	double[] totalCapacity = linkTable.getColumnAsDouble( "totalCapacity" );
    	double[] volad = linkTable.getColumnAsDouble( "volad" );

		for (int i=0; i < linkTable.getRowCount(); i++) {
			totalVolCapRatio[i] = (volau[i]+volad[i])/totalCapacity[i];
		}

		linkTable.setColumnAsDouble( linkTable.getColumnPosition("totalVolCapRatio"), totalVolCapRatio );
		
    }

    public void setFlows (double[][] flow) {
        for ( char c : userClasses ) {
            int m = getUserClassIndex(c);
            linkTable.setColumnAsDouble( linkTable.getColumnPosition("flow_" + c), flow[m] );
        }
    }
    
    public void setTtf ( int[] ttf ) {
        linkTable.setColumnAsInt( linkTable.getColumnPosition("ttf"), ttf );
    }
      
    private char[] getUserClassesFromProperties ( String userClassPropertyString ) {

		// get the mode codes that identify user classes
		ArrayList userClassList = new ArrayList();
		StringTokenizer st = new StringTokenizer(userClassPropertyString, ", |");
		while (st.hasMoreTokens()) {
			userClassList.add(st.nextElement());
		}

		// copy the valid mode codes into an array
		char tempUserClass;
		char[] tempUserClasses = new char[userClassList.size()];
		int j = 0;
		for (int i=0; i < tempUserClasses.length; i++) {
			tempUserClass = ((String)userClassList.get(i)).charAt(0);
			if ( tempUserClass == 'a' || tempUserClass == 'd' || tempUserClass == 'e' || tempUserClass == 'f' || tempUserClass == 'g' || tempUserClass == 'h' )
				tempUserClasses[j++] = tempUserClass;
		}

		// create an array of valid mode codes to return
		char[] finalUserClasses = new char[j];
		for (int i=0; i < finalUserClasses.length; i++) {
			finalUserClasses[i] = tempUserClasses[i];

			userClassMap.put ( String.valueOf(finalUserClasses[i]), Integer.valueOf(i) );
		}

		return finalUserClasses;

    }


    private float[] getUserClassPcesFromProperties ( String userClassPcesPropertyString ) {

        // get the mode codes that identify user classes
        ArrayList<String> userClassPcesList = new ArrayList();
        StringTokenizer st = new StringTokenizer(userClassPcesPropertyString, ", |");
        while (st.hasMoreTokens()) {
            userClassPcesList.add( (String)st.nextElement() );
        }

        float[] returnArray = new float[userClassPcesList.size()];
        for ( int i=0; i < userClassPcesList.size(); i++ )
            returnArray[i] = Float.parseFloat( userClassPcesList.get(i) );

        return returnArray;

    }


    /**
     * 
     * @param userClassVotPropertyString could be the property file key for peak or offpeak value of time or operating cost values
     * @return float[] with user class indexed values
     */
    private float[] getUserClassFloatValuesFromProperties ( String userClassPropertyString ) {

        // get the mode codes that identify user classes
        ArrayList<String> userClassValueList = new ArrayList<String>();
        StringTokenizer st = new StringTokenizer(userClassPropertyString, ", |");
        while (st.hasMoreTokens()) {
            userClassValueList.add( (String)st.nextElement() );
        }

        float[] returnArray = new float[userClassValueList.size()];
        for ( int i=0; i < userClassValueList.size(); i++ )
            returnArray[i] = Float.parseFloat( userClassValueList.get(i) );

        return returnArray;

    }


    /**
     * The char[][] returned by this class is used by Demand handling objects to determine
     * which truck modes are combined together into a userclass for assignment and therefore
     * which truck mode trips get combined into a single user class o/d matrix.
     */
    private char[][] setAssignmentGroups ( String[] truckClassPropertyValues ) {
    
        char tempUserClass;
        String truckClassPropertyString = null;
        StringTokenizer st = null;

        
        // count the number of truck assignment groups defined from as many as 5 possible groups
        int groupCount = 0;
        for  (int i=0; i < 5; i++ ) {
            truckClassPropertyString = truckClassPropertyValues[i+1];
            st = new StringTokenizer( truckClassPropertyString, ", |" );
            while (st.hasMoreTokens()) {
                tempUserClass = ((String)st.nextElement()).charAt(0);
                // if truck assignment group has any of the truck mode characters, count the group
                if ( tempUserClass == 'd' || tempUserClass == 'e' || tempUserClass == 'f' || tempUserClass == 'g' || tempUserClass == 'h' ) {
                    groupCount++;
                    break;
                }
            }
        }

        
        // allocate an array for the truck groups plus the auto group
        char[][] assignmentGroupChars = new char[groupCount+1][];

        
        
        // get the mode codes associated with each of the truck groups and put into a HashMap and an array
        for  (int i=0; i < groupCount; i++ ) {
            truckClassPropertyString = truckClassPropertyValues[i+1];
            
            // count mode chars in this truckClassPropertyString
            int modeCount = 0;
            st = new StringTokenizer( truckClassPropertyString, ", |" );
            while (st.hasMoreTokens()) {
                tempUserClass = ((String)st.nextElement()).charAt(0);

                if ( tempUserClass == 'd' || tempUserClass == 'e' || tempUserClass == 'f' || tempUserClass == 'g' || tempUserClass == 'h' )
                    modeCount++;
            }
            
            if ( modeCount > 0 ) {
            
                // dimension the assignmentGroupChars array for this truck assignmentGroup and store modeChars in array and assignmentGroupMap
                assignmentGroupChars[i+1] = new char[modeCount];
            
                st = new StringTokenizer( truckClassPropertyString, ", |" );
                int k = 0;
                while (st.hasMoreTokens()) {
                    tempUserClass = ((String)st.nextElement()).charAt(0);
                        
                    // if the mode code is a valid truck code, add it to the assignment group map.
                    if ( tempUserClass == 'd' || tempUserClass == 'e' || tempUserClass == 'f' || tempUserClass == 'g' || tempUserClass == 'h' ) {
                        assignmentGroupMap.put ( String.valueOf(tempUserClass), Integer.valueOf(i+1) );
                        assignmentGroupChars[i+1][k++] = tempUserClass;
                    }
                }
                
            }
            
        }

    	// if auto is in the list of user classes, add auto as assignment group 0.
    	if ( userClassesIncludeAuto () ) {
			assignmentGroupMap.put ( String.valueOf('a'), Integer.valueOf(0) );
        
			assignmentGroupChars[0] = new char[1];
			assignmentGroupChars[0][0] = 'a';
            
        }
        
        return assignmentGroupChars;
        
    }
    
    

	/**
	 * Use this method to get the largest node value
	 */
	private int getMaxNode () {

		maxNode = 0;
		for (int i=0; i < linkTable.getRowCount(); i++) {
	        
			int an = (int)linkTable.getValueAt( i+1, "anode" );
			int bn = (int)linkTable.getValueAt( i+1, "bnode" );
	        
			// set the value for the largest node number in the network
			if (an > maxNode)
				maxNode = an;
			if (bn > maxNode)
				maxNode = bn;

		}

		return maxNode;
		
	}


    
    private void readLinkAttributesCsvFile ( String filename ) {

        
        // read the extra link attributes file and update link attributes table values.
        if ( filename != null && ! filename.equals("") ) {

            try {
                
                OLD_CSVFileReader reader = new OLD_CSVFileReader();
                TableDataSet table = reader.readFile( new File(filename) );

                int[] tazs = new int[table.getRowCount()];
                int[] drops = new int[table.getRowCount()];
                int[] uniqueIds = new int[table.getRowCount()];
                double[] capacity = new double[table.getRowCount()];
                double[] originalCapacity = new double[table.getRowCount()];
                double[] totalCapacity = new double[table.getRowCount()];
                double[][] linkAttribCost = new double[numUserClasses][table.getRowCount()];
                String[] labels = new String[table.getRowCount()];
                String[] revMode = new String[table.getRowCount()];
                
                boolean tableContainsDropsColumn = ( table.getColumnPosition("DROPLINK") >= 0 );
                boolean tableContainsRevisedModeColumn = ( table.getColumnPosition("REVISED_MODES") >= 0 );


                // set the assignment period index
                int periodIndex = assignmentPeriod.equalsIgnoreCase("peak") ? 0 : 1;
                float[] valueOfTimeArray = assignmentPeriod.equalsIgnoreCase("peak") ? userClassVotPk : userClassVotOp;

                double[] dist = getDist();
                double[] totLinkCost = new double[table.getRowCount()];

                // traverse links and store attibutes in linktable
                for (int i=0; i < table.getRowCount(); i++) {
                    
                    try {
                        
                        int an = (int)table.getValueAt( i+1, "FNODE" );
                        int bn = (int)table.getValueAt( i+1, "TNODE" );
                        int uniqueId = (int)table.getValueAt( i+1, "UNIQID" );
                        int cap = (int)table.getValueAt( i+1, "CAPACITY" );
                        int taz = (int)table.getValueAt( i+1, "NEWTAZ" );

                        
                        // read the link cost fields by user class into the timeValueOfCost array
                        double linkCost = 0.0;
                        for ( char c : userClasses ) {
                            int m = getUserClassIndex(c);
                            linkAttribCost[m][i] = table.getValueAt( i+1, String.format("COST_%c", c) );

                            // cost parameter is (60/VOT) in units of  min/cent ( minutes/hr / cents/hr )
                            // dist parameter is OPERATING_COST / VOT in min/mile (i.e. TIME_PARAMETER * OPERATING_COST )
                            linkCost += (60.0/valueOfTimeArray[m]) * ( linkAttribCost[m][i] + userClassOpCost[m]*dist[i] );
                        }
                        totLinkCost[i] = linkCost;


                        int drop = 0;
                        if ( tableContainsDropsColumn )
                            drop = (int)table.getValueAt( i+1, "DROPLINK" );
        

                        
                        int dummy = 0;
                        if ( an == 48889 && bn == 48810 ) {
                            dummy = 1;
                        }
                        

                        int k = getLinkIndex(an,bn);
                        
                        tazs[k] = taz;
                        
                        
                        if ( tableContainsRevisedModeColumn )
                            revMode[k] = table.getStringValueAt( i+1, "REVISED_MODES" );
                        
                        int lanes = (int)linkTable.getValueAt( k+1, "lanes" );
                        originalCapacity[k] = cap;
                        capacity[k] = cap / volumeFactor;
                        
                        // the following variables are needed for the VDF Integrals definitions
                        totalCapacity[k] = capacity[k] * lanes;

                        labels[k] = an + "_" + bn;
                        drops[k] = drop;
                        uniqueIds[k] = uniqueId;

                    }
                    catch ( Exception e ) {
                        logger.error ( String.format("exception caught reading attributes file, record %d.", i+1 ), e );
                        throw new RuntimeException();
                    }

                }

                setTaz(tazs);
                setCapacity(capacity);
                setOriginalCapacity(originalCapacity);
                setTotalCapacity(totalCapacity);
                setLinkLabels(labels);
                setUniqueIds(uniqueIds);
                setDrops(drops);
                setTotalLinkCost(totLinkCost);
                setLinkAttribCosts(linkAttribCost);

                if ( tableContainsRevisedModeColumn )
                    setRevisedModes(revMode);
                
            }
            catch (IOException e) {
                logger.error ( "exception caught reading extra attributes file: " + filename, e );
            }
                        
        }

    }
    
                
    

	
	/**
	 * Use this method to read the Emme2 d211 text file format network into
	 * a simple data table.
	 */
	private TableDataSet deriveLinkAttributes () {

		int[] turnPenaltyIndex = new int[linkTable.getRowCount()];
        int[] ttf = new int[linkTable.getRowCount()];
		float[] length = new float[linkTable.getRowCount()];
		double[] lanes = new double[linkTable.getRowCount()];
		double[] totalVolCapRatio = new double[linkTable.getRowCount()];
        double[] capacity = new double[linkTable.getRowCount()];
        double[] totalCapacity = new double[linkTable.getRowCount()];
		double[] originalCapacity = new double[linkTable.getRowCount()];
		double[] freeFlowSpeed = new double[linkTable.getRowCount()];
		double[] congestedTime = new double[linkTable.getRowCount()];
		double[] vdfIntegral = new double[linkTable.getRowCount()];
		double[] freeFlowTime = new double[linkTable.getRowCount()];
		double[] oldTime = new double[linkTable.getRowCount()];
		double[] volad = new double[linkTable.getRowCount()];
		double[] gc = new double[linkTable.getRowCount()];
		double[][] flow = new double[userClasses.length][linkTable.getRowCount()];
		boolean[] centroid = new boolean[linkTable.getRowCount()];
		String[] centroidString = new String[linkTable.getRowCount()];
		validLinksForClass = new boolean[userClasses.length][linkTable.getRowCount()];
		validLinks = new boolean[linkTable.getRowCount()];
        volau = new double[linkTable.getRowCount()];
        totAonFlow = new double[linkTable.getRowCount()];

        
		Arrays.fill (volad, 0.0);
		Arrays.fill (validLinks, false);

		for (int i=0; i < linkTable.getRowCount(); i++) {

			int an = (int)linkTable.getValueAt( i+1, "anode" );
			int bn = (int)linkTable.getValueAt( i+1, "bnode" );
            
			if ( isCentroid(an) || isCentroid(bn) ) {
					centroid[i] = true;
					centroidString[i] = "true";
			}
			else {
				centroid[i] = false;
				centroidString[i] = "false";
			}
    

			// set speed and capacity fields based on values in ul1; set minimum link distance.
			String mode = linkTable.getStringValueAt( i+1, "mode" );
			float ul1 = linkTable.getValueAt( i+1, "ul1" );

			
			// can't have zero speed for highway links, so fix those here for now
            if ( ul1 == 0  ) {
                if ( mode.indexOf('a') >= 0 )
                    ul1 = 15;
                else
                    ul1 = 5;
            }
            

            // set default values for capacity.
            // these will be overridden by values in an extra attributes link file, if such a file is specified.
            if ( centroid[i] )
                capacity[i] = 9999;
            else if (ul1 > 15 && ul1 <= 30)
                capacity[i] = 800;
            else if (ul1 > 30 && ul1 <= 40)
                capacity[i] = 1200;
            else if (ul1 > 40 && ul1 <= 50)
                capacity[i] = 1400;
            else if (ul1 > 50 && ul1 <= 60)
                capacity[i] = 1600;
            else if (ul1 > 60 && ul1 <= 70)
                capacity[i] = 1800;
            else if (ul1 > 70)
                capacity[i] = 2000;
            else
                capacity[i] = 600;

            
			
			lanes[i] = linkTable.getValueAt( i+1, "lanes" );
			
			originalCapacity[i] = capacity[i];
			capacity[i] /= volumeFactor;

			
			// the following variables are needed for the VDF Integrals definitions
			totalCapacity[i] = capacity[i] * lanes[i];
			totalVolCapRatio[i] = 0.0;

            
			float dist = linkTable.getValueAt( i+1, "dist" );
			if (dist == 0.0)
			    dist = 0.001f;
			length[i] = dist;




            // initialize the flow by user class fields to zero
			for (int j=0; j < userClasses.length; j++) {
				flow[j][i] = 0.0f;
			}
	        
			
			// The following modes are valid for TLUMIP Statewide network and multiclass highway assignment (6 classes).
			// Auto trips ares available on all highway links.
			// Truck trips are classed by weight, d being lightest to h being heaviest.
			// Lighter trucks are valid on all heavier class links 
			// a	auto
			// d	truck below 34k lbs.
			// e	truck between 64k lbs. and 34k lbs.
			// f	truck between 80k lbs. and 64k lbs.
			// g	truck between 105.5k lbs. and 80k lbs.
			// h	truck greater than 105.5k lbs.
			
			// initialize the valid links for multiclass network used in determining shortest paths by class
			for (int j=0; j < userClasses.length; j++) {
			    if ( mode.indexOf( userClasses[j] ) >= 0 ) {
			    	validLinksForClass[j][i] = true;
			    	validLinks[i] = true;
			    }
			    else {
			    	validLinksForClass[j][i] = false;
			    }
			}
	        

            if ( validLinks[i] && totalCapacity[i] == 0.0 ) {
                logger.error ( "check capacity for link " + i + ": [" + an + "," + bn + "], capacity=" + capacity[i] + ", lanes=" + lanes[i] + "." );
            }
            
            

			freeFlowSpeed[i] = ul1;
			congestedTime[i] = (float)((dist/ul1)*60.0);
			freeFlowTime[i] = congestedTime[i];
			oldTime[i] = congestedTime[i];
			float ul3 = (float)((dist/ul1)*60.0);

			linkTable.setValueAt( i+1, linkTable.getColumnPosition("ul1"), ul1);
			linkTable.setValueAt( i+1, linkTable.getColumnPosition("ul3"), ul3);

		}
	    
	    
        // apply any link mods for ul3 as done for PT (see times.mac)
        if (linkModsTable != null) {

            for (int i=0; i < linkModsTable.getRowCount(); i++) {
            
                int[] ib = getIb();
                int an = (int)linkModsTable.getValueAt( i+1, "anode" );
                int bn = (int)linkModsTable.getValueAt( i+1, "bnode" );
                int ia = nodeIndex[an];
                float ul3 = linkModsTable.getValueAt( i+1, "ul3" );
                
                for (int j=ipa[ia]; j < ipa[ia+1]; j++) {
                    int k = sortedLinkIndexA[j];
                    if (indexNode[ib[k]] == bn) {
                        linkTable.setValueAt( k+1, linkTable.getColumnPosition("ul3"), ul3);
                        linkTable.setValueAt( k+1, linkTable.getColumnPosition("vdf"), 3);
                        congestedTime[k] = ul3;
                        break;
                    }
                }
    
            }
            
            linkModsTable = null;
        
        }
        


		TableDataSet derivedTable = new TableDataSet();
	    
		derivedTable.appendColumn(centroidString, "centroid");
		derivedTable.appendColumn(turnPenaltyIndex, "turnPenaltyIndex");
		derivedTable.appendColumn(ttf, "ttf");
		derivedTable.appendColumn(totalVolCapRatio, "totalVolCapRatio");
		derivedTable.appendColumn(totalCapacity, "totalCapacity");
		derivedTable.appendColumn(capacity, "capacity");
		derivedTable.appendColumn(originalCapacity, "originalCapacity");
		derivedTable.appendColumn(freeFlowSpeed, "freeFlowSpeed");
		derivedTable.appendColumn(congestedTime, "congestedTime");
		derivedTable.appendColumn(congestedTime, "transitTime");
		derivedTable.appendColumn(volau, "volau");
		derivedTable.appendColumn(volad, "volad");
		derivedTable.appendColumn(gc, "generalizedCost");
		derivedTable.appendColumn(vdfIntegral, "vdfIntegral");
		derivedTable.appendColumn(freeFlowTime, "freeFlowTime");
		derivedTable.appendColumn(length, "length");
		derivedTable.appendColumn(oldTime, "oldTime");
		for (int j=0; j < userClasses.length; j++) {
			derivedTable.appendColumn(flow[j], "flow" + "_" + userClasses[j]);
		}

		
		return derivedTable;

	}

	
	
//	public void applyVdfs () {
//		
//			double[] results = fdLc.solve();
//			
//			for (int i=0 ; i < results.length; i++) {
//				if ( results[i] < 0 || results[i] == Double.NaN ) {
//					logger.error ( "invalid result in Network.applyVdfs().   results[i=" + i + "] = " + results[i] );
//					logger.error ( "anode = " + indexNode[ia[i]] + ", bnode = " + indexNode[ib[i]] );
//					System.exit(-1);
//				}
//			}
//				
//			linkTable.setColumnAsDouble( linkTable.getColumnPosition("congestedTime"), results );
//
//		}
		
		
		
	public void applyVdfs () {
		
        double[] congestedTime = (double[])linkTable.getColumnAsDouble( "congestedTime" );
        
		double[] results = fdLc.solve(validLinks);
		
		for (int i=0 ; i < results.length; i++) {
            
            // if link calculater returns a negative or NaN result for a valid link, report the error
            if ( validLinks[i] ) {

                if ( results[i] < 0 || (Double.valueOf(results[i])).equals(Double.NaN) ) {
                    logger.error ( "invalid result in Network.applyVdfs(boolean[] validLinks).   results[i=" + i + "] = " + results[i] );
                    logger.error ( "anode = " + indexNode[ia[i]] + ", bnode = " + indexNode[ib[i]] );
                    logger.error ( "volau = " + linkTable.getValueAt( i+1, linkTable.getColumnPosition("volau") ) );
                    logger.error ( "volad = " + linkTable.getValueAt( i+1, linkTable.getColumnPosition("volad") ) );
                    logger.error ( "capacity = " + linkTable.getValueAt( i+1, linkTable.getColumnPosition("capacity") ) );
                    logger.error ( "lanes = " + linkTable.getValueAt( i+1, linkTable.getColumnPosition("lanes") ) );
                    logger.error ( "length = " + linkTable.getValueAt( i+1, linkTable.getColumnPosition("length") ) );
                    //throw new RuntimeException();
                }
                else {
                    congestedTime[i] = results[i];
                }
            }
		}
			
		linkTable.setColumnAsDouble( linkTable.getColumnPosition("congestedTime"), congestedTime );

	}
		
		
		
	public void applyVdfIntegrals () {
	
		double[] results = fdiLc.solve(validLinks);
		
		for (int i=0 ; i < results.length; i++) {
            // if link calculater returns a negative or NaN result for a valid link, report the error
            if ( validLinks[i] ) {
                if ( results[i] == Double.NaN ) {
                    logger.error ( "invalid result in Network.applyVdfIntegrals(boolean[] validLinks).   results[i=" + i + "] = " + results[i] );
                    logger.error ( "anode = " + indexNode[ia[i]] + ", bnode = " + indexNode[ib[i]] );
                    System.exit(-1);
                }
			}
            // the integral of a non-valid link is the constant, default time times the highway flow, which is zero, so the result is 0. 
            else {
                results[i] = 0.0;
            }
		}
			
		linkTable.setColumnAsDouble( linkTable.getColumnPosition("vdfIntegral"), results );

	}
	
	
	public double applyLinkTransitVdf ( int hwyLinkIndex, int transitVdfIndex ) {
		
        double result = -1.0;
        
		// calculate the link in-vehicle travel times based on the transit vdf index for the link passed in
        try {

            result = ftLc.solve(hwyLinkIndex, transitVdfIndex);
		
    		if ( result < 0 || result == Double.NaN )
                throw new RuntimeException();
            
        }
        catch (RuntimeException e) {
            logger.error ( "invalid result in Network.applyLinkTransitVdf(int hwyLinkIndex, int transitVdfIndex).   hwyLinkIndex=" + hwyLinkIndex + ", transitVdfIndex = " + transitVdfIndex + ", result = "+ result );
            throw new RuntimeException(e);
        }
        
		return result;
		
	}

	
	private void setInternalNodeNumbering () {

		int[] ia = new int[linkTable.getRowCount()];
		int[] ib = new int[linkTable.getRowCount()];

        int[] an = linkTable.getColumnAsInt( "anode" );
        int[] bn = linkTable.getColumnAsInt( "bnode" );

		maxNode = getMaxNode();


        int count = 0;
        HashMap<Integer, Integer> tempHashMap = new HashMap<Integer, Integer>();

        /* check for duplicate node entries in node table
        for ( int i : getNodes() ) {
            count = 0;
            if ( tempHashMap.containsKey( i ))
                count = tempHashMap.get( i );
            tempHashMap.put( i, ++count );
        }
        count = 0;
        for ( int i : tempHashMap.keySet() ) {
            if ( tempHashMap.get( i ) == 0 )
                count++;
        }
		*/
		
        
        /* check for nodes with multiple links entering, none leaving and multiple links leaving, but none entering
		int aCount = 0;
		HashMap<Integer, Integer> aTempHashMap = new HashMap<Integer, Integer>();
        for ( int i : an ) {
            aCount = 0;
            if ( aTempHashMap.containsKey( i ))
                aCount = aTempHashMap.get( i );
            aTempHashMap.put( i, ++aCount );
        }

        int bCount = 0;
        HashMap<Integer, Integer> bTempHashMap = new HashMap<Integer, Integer>();
        for ( int i : bn ) {
            bCount = 0;
            if ( bTempHashMap.containsKey( i ))
                bCount = bTempHashMap.get( i );
            bTempHashMap.put( i, ++bCount );
        }
        
        aCount = 0;
        for ( int i : aTempHashMap.keySet() ) {
            if ( aTempHashMap.get( i ) > 1 && ! bTempHashMap.containsKey( i ) )
                aCount++;
        }
        bCount = 0;
        for ( int i : bTempHashMap.keySet() ) {
            if ( bTempHashMap.get( i ) > 1 && ! aTempHashMap.containsKey( i ) )
                bCount++;
        }
         */
        
        
        
		indexNode = new int[nodeTable.getRowCount()+1];
		nodeIndex = new int[maxNode+1];
		int[] usedNodes = new int[maxNode+1];
        
		Arrays.fill ( indexNode, 0 );
		Arrays.fill ( nodeIndex, -1 );
		Arrays.fill ( usedNodes, -1 );


		// renumber centroid nodes first
		int nextNode = 0;
		numCentroids = 0;
		maxCentroid = 0;
		for (int i=0; i < linkTable.getRowCount(); i++) {
            
			if ( isCentroid(an[i]) && usedNodes[an[i]] < 0) {
			    if (an[i] > maxCentroid)
			        maxCentroid = an[i];
				usedNodes[an[i]] = nextNode++;
			    numCentroids++;
			}

			if ( isCentroid(bn[i]) && usedNodes[bn[i]] < 0) {
			    if (bn[i] > maxCentroid)
			        maxCentroid = bn[i];
				usedNodes[bn[i]] = nextNode++;
			    numCentroids++;
			}

		}

		
		// now renumber regular nodes and set the internal node number array and correspondence array values
		for (int i=0; i < linkTable.getRowCount(); i++) {
            
			if (usedNodes[an[i]] < 0)
				usedNodes[an[i]] = nextNode++;
			if (usedNodes[bn[i]] < 0)
				usedNodes[bn[i]] = nextNode++;

			ia[i] = usedNodes[an[i]];
			ib[i] = usedNodes[bn[i]];

			indexNode[ia[i]] = an[i];
			nodeIndex[an[i]] = ia[i];
			indexNode[ib[i]] = bn[i];
			nodeIndex[bn[i]] = ib[i];

		}
        
        
		
		// add new link node fields to TableDataSet
		linkTable.appendColumn( ia, "ia" );
		linkTable.appendColumn( ib, "ib" );

	}
    
    
	private int[] setForwardStarArrays ( int[] nodes, int[] indexArray ) {
	    
		int k;
		int old;

		int[] ip = new int[nodeTable.getRowCount()+1];
		
		old = nodes[indexArray[0]];
		ip[old] = 0;
		for (int i=0; i < nodes.length; i++) {
			k = indexArray[i];
			
			if ( nodes[k] != old ) {
				ip[nodes[k]] = i;
				old = nodes[k];
			}
		}
		ip[old+1] = nodes.length;
		
		/* count number of nodes which have no children
		int count = 0;
		for ( int i=0; i < ip.length; i++ )
		    if ( ip[i] == 0 )
		        count++;
		*/
		
		return ip;

	}

	
	
	public void checkForIsolatedLinks () {
	    
		int start;
		int end;
		int aExit;
		int bExit;
        
        boolean[] centroid = getCentroid();
		boolean errorsFound = false;

        ArrayList msgList = new ArrayList();
        int[] msgNodes = new int[indexNode.length];
        int[] msgNodeUsed = new int[indexNode.length];
        Arrays.fill(msgNodes,NOT_USED_FLAG);
        Arrays.fill(msgNodeUsed,NOT_USED_FLAG);
        
        

        int k = 0;
		for (int userClass = 0; userClass < userClasses.length; userClass++) {
			
			logger.info ( "checking network connectivity for userClass " + userClass + " (" + userClasses[userClass] + ") subnetwork.");
			
			// for each anode that has only one bnode, flag links where bnode only goes back to anode.
			for (int i=0; i < ia.length; i++) {
                
				if ( !validLinksForClass[userClass][i] )
					continue;
				
				aExit = -1;
				bExit = -1;

				start = ipa[ia[i]];
				end = ipa[ia[i]+1];
				
				// if anode has only one exiting link
				if (start == end - 1) {
					
					aExit = sortedLinkIndexA[start];
					
					start = ipa[ib[i]];
					end = ipa[ib[i]+1];
					
                    // if bnode has only one exiting link
                    if (start == end - 1) {
                        
                        bExit = sortedLinkIndexA[start];
                        
                        // if the node pair is same in each direction, this is an isolated link:
                        if (ia[aExit] == ib[bExit] && ib[aExit] == ia[bExit]) {
                            logger.error ( "link pair " + aExit + "[" + indexNode[ia[aExit]] + "," + indexNode[ib[aExit]] + "], UNIQID=" + uniqueIds[aExit] + " and " + bExit + "[" + indexNode[ia[bExit]] + "," + indexNode[ib[bExit]] + "], UNIQID=" + uniqueIds[bExit] + " is disconnected from network.");
                            errorsFound = true;
                        }
                    
                    }


                    start = ipb[ia[i]];
                    end = ipb[ia[i]+1];
                    
                    // if anode has only one entering link, and that link is the opposite direction of current link:
                    if (start == end - 1) {
                        
                        bExit = sortedLinkIndexB[start];
                        
                        // if the node pair is same in each direction and links are not centroid connectors, these are hanging links
                        if (!centroid[i] && ia[aExit] == ib[bExit] && ib[aExit] == ia[bExit]) {
                            if ( msgNodeUsed[ia[aExit]] == NOT_USED_FLAG ) {
                                msgList.add ( "node " + indexNode[ia[aExit]] + " is dangling from the network.  link pair " + bExit + "[" + indexNode[ia[bExit]] + "," + indexNode[ib[bExit]] + "] and " + aExit + "[" + indexNode[ia[aExit]] + "," + indexNode[ib[aExit]] + "], UNIQIDs=" + uniqueIds[bExit] + "," + uniqueIds[aExit] + ".");
                                msgNodes[k++] = indexNode[ia[aExit]];
                                msgNodeUsed[ia[aExit]] = indexNode[ia[aExit]];
                            }
                        }
                    
                    }
                    
				}
				
			}

		}
			
        if (errorsFound) {
            logger.error ( "errors identified above in constructing the internal network representations.");
            logger.error ( "one or more node pairs identified above are connected only to each other for the");
            logger.error ( "specified subnetwork in which case they are isolated and no subnetwork paths may");
            logger.error ( "be built through these nodes and links.\n\n" );
        }

        
        
        int[] msgIndices = IndexSort.indexSort( msgNodes );
        
        for (int i=0; i < msgList.size(); i++) {
            logger.error( (String)msgList.get(msgIndices[i]) );
        }
        
        if ( msgList.size() > 0 ) {
            logger.error ( "errors identified above in constructing the internal network representations.");
            logger.error ( "one or more dangling nodes were found where only a subnetwork path that returns");
            logger.error ( "to the node from which it came is possible.\n\n");
        }

	}

	
	
    private void setOnewayLinks () {
        
        onewayLinksForClass = new int[userClasses.length][ia.length];
        
        for (int userClass=0; userClass < userClasses.length; userClass++) {
            
            for (int i=0; i < ia.length; i++) {
                
                // mark ia and ib value of link
                int iaTemp = ia[i];
                int ibTemp = ib[i];

                // initialize link to oneway to 0 (false), the default value in case it's not a valid link for the userclass. 
                int oneway = 0;
                
                if ( !validLinksForClass[userClass][i] )
                    continue;
                
                // set its oneway value to 1, the asserted value, which will be changed to 0 if an opposite drection link is found.
                oneway = 1;
                
                // loop over links exiting bnode of link i
                for (int j=ipa[ibTemp]; j < ipa[ibTemp+1]; j++) {
                    
                    int k = sortedLinkIndexA[j];
                    
                    // if the ia and ib of this link equals the ib and ia of the original link, then link i is two-way.
                    if ( ia[k] == ibTemp && ib[k] == iaTemp ) {
                        oneway = 0;
                        break;
                    }
                    
                }
                
                onewayLinksForClass[userClass][i] = oneway;
                
            }

        }

    }



    private void setDroppedLinks () {
        
        for (int i=0; i < ia.length; i++) {
            
            if ( drops[i] == 1 )
                for (int m=0; m < validLinksForClass.length; m++)
                    validLinksForClass[m][i] = false;
            
        }

    }



	private void setTurnPenalties( 	float[][] turnDefs ) {

		int k=0;

		ArrayList[] turnLists = new ArrayList[linkTable.getRowCount()];
        turnTable = new float[linkTable.getRowCount()][][];
        turnPenaltyIndices = new int[linkTable.getRowCount()][];
        turnPenaltyArray = new float[linkTable.getRowCount()][];
		

		// set the turn penalty index record for each link that is downstream in the turning movement.
		// this record provides the turn penalty function information used to define the turn penalty/prohibition.
		for (int i=0; i < turnDefs.length; i++) {
            
			// turning movement i->j->k is coded in file as j i k
			k = getLinkIndex ( (int)turnDefs[i][0], (int)turnDefs[i][2] );

			if (turnLists[k] == null)
				turnLists[k] = new ArrayList();

			turnLists[k].add(turnDefs[i]);
			
		}

        
        // turnLists[k] is an ArrayList of the turn penalty file records with link k as the downstream link
        // the elements in the ArrayList are arrays of node sequence and penalty for a turn into link k
        // the node sequence is: j, i, k for a turn defined as i->j->k.
        // therfore, turnLists[k][n][0] = the j node for the nth turn record going into link k
        //           turnLists[k][n][1] = the i node for the nth turn record going into link k
        //           turnLists[k][n][2] = the k node for the nth turn record going into link k
        //           turnLists[k][n][3] = the penalty for turn i->j->k.
		for (k=0; k < linkTable.getRowCount(); k++) {

			if (turnLists[k] != null) {
                
				turnTable[k] = new float[turnLists[k].size()][((float[])turnLists[k].get(0)).length];
                turnPenaltyIndices[k] = new int[turnLists[k].size()];
                turnPenaltyArray[k] = new float[turnLists[k].size()];
                for (int j=0; j < turnLists[k].size(); j++) {
                    
                    turnTable[k][j] = (float[])turnLists[k].get(j);
//                    int link2Index = getLinkIndex ( (int)turnTable[k][j][0], (int)turnTable[k][j][2] );
                    turnPenaltyIndices[k][j] = getLinkIndex ( (int)turnTable[k][j][1], (int)turnTable[k][j][0] );
                    turnPenaltyArray[k][j] = turnTable[k][j][3];
                                                       
                }
			}
		}

	}

	
	public double getTurnPenalty ( int jn, int in, int kn ) {

		double returnValue = 0;
		
		// if turnTable is null, no penalties or prohibitions at all are defined
		if (turnTable == null)
		    return 0;
	    
		
		// get the turn penalty index from the turn penalty record if one exists
		// if index == -1, no penalty 
		// if index ==  0, turn prohibited 
		// if index >=  1, turn penalty function index (functions defined in d411 format file) 
		int fpIndex = getTurnPenaltyIndex ( jn, in, kn );
		
		// if this downstream link is not part of a penalized or prohibited turn, return penalty is zero.
		if ( fpIndex < 0 )
			return 0;
		// else, if this downstream link is part of a prohibited turn, return penalty is -1.
		else if ( fpIndex == 0 )
			return -1;
		
		
		// otherwise, this downstream link is part of a penalized turn so return the turn penalty.
		int k = getLinkIndex ( jn, kn );
		returnValue = fpLc.solve( k, fpIndex );

		
		return returnValue;
		
	}
	
	
	private int getTurnPenaltyIndex ( int jn, int in, int kn ) {

		int returnValue = -1;
	    
		int k = getLinkIndex ( jn, kn );
		
		// if this downstream link is not in turn table, the turn is non penalized,
		// so return -1.
		if ( turnTable[k] == null )
			return returnValue;
		
		
		// if this downstream link is in turn table, check to see if the turn is penalized or prohibited.
		// if prohibited, return 0.  if penalized, return the turn penalty function index.
		float[][] linkTurnTable = turnTable[k];
		
		for (int i=0; i < linkTurnTable.length; i++) {

			if ( (int)linkTurnTable[i][0] == jn && (int)linkTurnTable[i][1] == in && (int)linkTurnTable[i][2] == kn ) {
				returnValue = (int)linkTurnTable[i][3];
				break;
			}
		    
		}

		return returnValue;
		
	}
	
	
    public int getLinkIndex( int an, int bn ) {

        // takes external node numbers and returns link index
        int i = 0;
        int k = 0;
        int start = 0;
        int end = 0;
        int offset = 0;

        int returnValue = -1;
        
        // search through bnodes for the given anode to find the link and return its index
        try {

            // if an is not in network, link is not either
            if ( nodeIndex[an] < 0 )
                return -1;
            
            start = ipa[nodeIndex[an]];
            
            offset = 1;
            end = ipa[nodeIndex[an] + offset++];
            while ( end <= 0 )
                end = ipa[nodeIndex[an] + offset++];

            for (i=start; i < end; i++) {
                
                k = sortedLinkIndexA[i];
                
                if(logger.isDebugEnabled()) {
                    logger.debug ("i=" + i + ", k=" + k + ", an=" + an + ", bn=" + bn + ", ia=" + nodeIndex[an] + ", ib=" + nodeIndex[bn] + ", start=" + start + ", end=" + end + ", offset=" + offset );
                }
                
                if ( ib[k] == nodeIndex[bn] ) {
                    returnValue = k;
                    break;
                }
            }

        }
        catch ( Exception e ) {
            logger.error ("problem getting link id for: an=" + an + ", bn=" + bn + "." );
            logger.error ("i=" + i + ", k=" + k + ", an=" + an + ", bn=" + bn + ", ia=" + nodeIndex[an] + ", ib=" + nodeIndex[bn] + ", start=" + start + ", end=" + end + ", offset=" + offset );
            throw new RuntimeException();
        }
        

        
//        if ( returnValue == 0 ) {
//            
//            // search through anodes for the given bnode to find the link and return its index
//            start = ipb[nodeIndex[bn]];
//            end = ipb[nodeIndex[bn] + 1];
//            if ( end <= 0 )
//                end = start + 1;
//
//            for (int i=start; i < end; i++) {
//                
//                k = sortedLinkIndexB[i];
//                
//                if(logger.isDebugEnabled()) {
//                    logger.debug ("i=" + i + ", k=" + k + ", an=" + nodeIndex[an] + ", bn=" + nodeIndex[bn] + ", ia[k=" + k + "]=" + ia + ", ib[k=" + k + "]=" + ib );
//                }
//                
//                if ( ia[k] == nodeIndex[an] ) {
//                    returnValue = k;
//                    break;
//                }
//            }
//
//        }
        

        return returnValue;
        
    }

    
    
    /** takes external node number and returns link indices entering that node
     * 
     * @param n external node number.
     * @return array of link indices for links entering node n.
     */
    public int[] getLinksEnteringNode( int n ) {

        // search through anodes for the given bnode to collect link indices in an array
        int offset = 1;
        int start = ipb[nodeIndex[n]];
        int end = ipb[nodeIndex[n] + offset++];
        while ( end <= 0 )
            end = ipb[nodeIndex[n] + offset++];

        // create an array to hold link indices returned
        int numLinksEntering = end - start;
        int[] result = new int[numLinksEntering];

        // loop through liks entering n and store link indices.
        int count = 0;
        for (int i=start; i < end; i++) {
            int k = sortedLinkIndexB[i];
            
            int an = indexNode[ia[k]];
            int id = getLinkIndex( an, n );
            result[count++] = id;
        }

        return result;
        
    }

    
    
    /** takes external node number and returns link indices exiting that node
     * 
     * @param n external node number.
     * @return array of link indices for links exiting node n.
     */
    public int[] getLinksExitingNode( int n ) {

        // search through bnodes for the given anode to collect link indices in an array
        int offset = 1;
        int start = ipa[nodeIndex[n]];
        int end = ipa[nodeIndex[n] + offset++];
        while ( end <= 0 )
            end = ipa[nodeIndex[n] + offset++];

        // create an array to hold link indices returned
        int numLinksEntering = end - start;
        int[] result = new int[numLinksEntering];

        // loop through liks entering n and store link indices.
        int count = 0;
        for (int i=start; i < end; i++) {
            int k = sortedLinkIndexA[i];
            
            int bn = indexNode[ib[k]];
            int id = getLinkIndex( n, bn );
            result[count++] = id;
        }

        return result;
        
    }

    
    
    public void linkSummaryReport ( double[][] flow ) {
        
        double totalVol;
        double[][] volumeSum = new double[numUserClasses][Constants.MAX_LINK_TYPE];
        int[] typeFreq = new int[Constants.MAX_LINK_TYPE];
        double[] typeTime1 = new double[Constants.MAX_LINK_TYPE];
        double[] typeTime2 = new double[Constants.MAX_LINK_TYPE];
        double[] typeTime3 = new double[Constants.MAX_LINK_TYPE];
        double[] typeTime4 = new double[Constants.MAX_LINK_TYPE];

        //String indexString = "Link Type";
        //int[] linkType = getLinkType();
        String indexString = "Link VDF";
        int[] linkType = getVdfIndex();

        double[] congestedTime = (double[])linkTable.getColumnAsDouble( "congestedTime" );
        double[] freeFlowSpeed = (double[])linkTable.getColumnAsDouble( "freeFlowSpeed" );
        double[] distance =      (double[])linkTable.getColumnAsDouble( "dist" );
        double[] capacity =      (double[])linkTable.getColumnAsDouble( "capacity" );
        

        for (int k=0; k < numLinks; k++) {
            typeFreq[linkType[k]]++;
            typeTime1[linkType[k]] += congestedTime[k];
            typeTime2[linkType[k]] += distance[k];
            typeTime3[linkType[k]] += freeFlowSpeed[k];
            typeTime4[linkType[k]] += capacity[k];
            for (int m=0; m < numUserClasses; m++)
                volumeSum[m][linkType[k]] += flow[m][k];
        }

        logger.info("");
        logger.info("");
        logger.info("");
        String logRecord = String.format("%-10s %10s", indexString, "Num Links");
        for (int m=0; m < numUserClasses; m++)
            logRecord += String.format("        class %c", userClasses[m]);
        logRecord += "          total";
        logRecord += "        avgTime";
        logRecord += "        avgDist";
        logRecord += "         avgSpd";
        logRecord += "       avgFfspd";
        logRecord += "    avgCapacity";
        logger.info( logRecord );
        for (int i=0; i < Constants.MAX_LINK_TYPE; i++) {
            totalVol = 0.0;
            for (int m=0; m < numUserClasses; m++)
                totalVol += volumeSum[m][i];
            if (totalVol > 0.0) {
                double avg1 = typeTime1[i]/typeFreq[i];
                double avg2 = typeTime2[i]/typeFreq[i];
                double avg3 = typeTime3[i]/typeFreq[i];
                double avg4 = typeTime4[i]/typeFreq[i];
                logRecord = String.format("%-10d %10d", i, typeFreq[i]);
                for (int m=0; m < numUserClasses; m++)
                    logRecord += String.format("   %12.2f", volumeSum[m][i]);
                logRecord += String.format("   %12.2f", totalVol);
                logRecord += String.format("   %12.2f", avg1);
                logRecord += String.format("   %12.2f", avg2);
                logRecord += String.format("   %12.2f", (avg2/(avg1/60.0)) );
                logRecord += String.format("   %12.2f", avg3);
                logRecord += String.format("   %12.2f", avg4);
                logger.info( logRecord );
            }
        }
    }

    
    
    public void logLinkTimeFreqs () {
	    
		int[] ia = getIa();
		int[] ib = getIb();
        
		double[] congestedTime = (double[])linkTable.getColumnAsDouble( "congestedTime" );
		int[] buckets = new int[8];

		for (int i=0; i < congestedTime.length; i++) {
			
			if ( !validLinks[i] )
				continue;
			
			if ( congestedTime[i] > 0 && congestedTime[i] <= 1.0 )
				buckets[0]++;
			else if ( congestedTime[i] > 1.0 && congestedTime[i] <= 10.0 )
				buckets[1]++;
			else if ( congestedTime[i] > 10.0 && congestedTime[i] <= 100.0 )
				buckets[2]++;
			else if ( congestedTime[i] > 100.0 && congestedTime[i] <= 1000.0 )
				buckets[3]++;
			else if ( congestedTime[i] > 1000.0 )
				buckets[4]++;
			else if ( congestedTime[i] == 0.0 ) {
				buckets[5]++;
				logger.info ( i + ": (" + indexNode[ia[i]] + "," + indexNode[ib[i]] + ") has congested time = " + congestedTime[i] );
			}
			else {
				buckets[6]++;
				logger.info ( i + ": (" + indexNode[ia[i]] + "," + indexNode[ib[i]] + ") has congested time = " + congestedTime[i] );
			}
		}

		logger.info ("");
		logger.info ( "frequency table of free flow link travel times");
		logger.info ( "for links valid for at least one user class");
		logger.info ( buckets[5] + " == 0");
		logger.info ( "0 < " + buckets[0] + " <= 1.0");
		logger.info ( "1.0 < " + buckets[1] + " <= 10.0");
		logger.info ( "10.0 < " + buckets[2] + " <= 100.0");
		logger.info ( "100.0 < " + buckets[3] + " <= 1,000.0");
		logger.info ( buckets[4] + " > 1,000.0");
		logger.info ( buckets[6] + " other");
		logger.info ("");
		
	}
	
	

	public void writeHighwayAsignmentResults ( String fileName ) {

		int[] ia = getIa();
		int[] ib = getIb();
		double[] congestedTime = (double[])linkTable.getColumnAsDouble( "congestedTime" );
		double[] capacity = (double[])linkTable.getColumnAsDouble( "capacity" );

        double[][] flow = new double[userClasses.length][];
        double[][] cost = new double[userClasses.length][];
		for ( char c : userClasses ) {
            int j = getUserClassIndex(c);
            flow[j] = (double[])linkTable.getColumnAsDouble( "flow_" + c );
            cost[j] = (double[])linkTable.getColumnAsDouble( "linkAttribCosts_" + c );
		}
		
		
		PrintWriter outStream = null;

        // open output stream for network attributes file
		try {
			
			outStream = new PrintWriter (new BufferedWriter( new FileWriter(fileName) ) );

			
            String tempString = String.format( "id,%s,%s,%s,%s", OUTPUT_ANODE_FIELD, OUTPUT_BNODE_FIELD, OUTPUT_CAPACITY_FIELD, OUTPUT_TIME_FIELD );
            
            for (int j=0; j < userClasses.length; j++)
                tempString += String.format( ",%s_%c", OUTPUT_FLOW_FIELDS_START_WITH, userClasses[j] );

            for (int j=0; j < userClasses.length; j++)
                tempString += String.format( ",%s_%c", OUTPUT_COST_FIELDS_START_WITH, userClasses[j] );

			outStream.println( tempString );
			

			
			for (int k=0; k < ia.length; k++) {
			
                tempString = String.format("%d,%d,%d,%.4f,%.4f", k, indexNode[ia[k]], indexNode[ib[k]], capacity[k], congestedTime[k]);
								
                for (int j=0; j < userClasses.length; j++) {
                    float pce = userClassPces[j];
                    double tempFlow = flow[j][k]/pce;
                    tempString += String.format(",%.4f", tempFlow);
                }

                for (int j=0; j < userClasses.length; j++)
                    tempString += String.format(",%.4f", cost[j][k]);

				outStream.println( tempString );

			}
		
			outStream.close();

		
		}
		catch (IOException e) {
			logger.fatal ("I/O exception writing assignment results file.", e);
		}

	}
	

    private boolean isValidDoubleValue( double value ) {
        return ( value >= -Double.MAX_VALUE && value <= Double.MAX_VALUE );
    }

    
    public int[] getExternalZoneLabels () {
     
        //TODO: get this list from the correspondence file between 5000 zones and 6000 zones
        int[] externalNumbers = { 5001, 5002, 5003, 5004, 5005, 5006, 5007, 5008, 5009, 5010, 5011, 5012 };
        
        return externalNumbers;
        
    }

    
    
    public int[] getAlphaDistrictArray() {
        return alphaDistrictIndex;
    }
    

    public String[] getDistrictNames() {
        return districtNames;
    }
    

    private void createAlphaDistrictArray( String zoneIndexFile ) {
        
        int districtIndex = 0;
        HashMap districtNameIndex = new HashMap();

        
        alphaDistrictIndex = new int[maxCentroid+1];
        
        // get the filename for the alpha/beta zone correspondence file and 
        // create an object for defining the extent of the study area
        try {
            
            OLD_CSVFileReader reader = new OLD_CSVFileReader();
            reader.setDelimSet( "," + reader.getDelimSet() );
            TableDataSet a2b = reader.readFile(new File(zoneIndexFile));

            int tazPosition = a2b.getColumnPosition("Azone");
            if (tazPosition <= 0) {
                logger.fatal( String.format("%s was not a field in the alpha/beta zone correspondence file: %s.", "Azone", zoneIndexFile));
                throw new RuntimeException();
            }

            int districtPosition = a2b.getColumnPosition("MPOmodeledzones");
            if (districtPosition <= 0) {
                logger.fatal( String.format("%s was not a field in the alpha/beta zone correspondence file: %s.", "MPOmodeledzones", zoneIndexFile));
                throw new RuntimeException();
            }

            for (int j = 1; j <= a2b.getRowCount(); j++) {
                int taz = (int)a2b.getValueAt(j, tazPosition);
                String districtName = a2b.getStringValueAt(j, districtPosition);
                
                if ( districtNameIndex.containsKey( districtName ) ) {
                    int index = (Integer)districtNameIndex.get(districtName);
                    alphaDistrictIndex[taz] = index;
                }
                else {
                    districtNameIndex.put(districtName, districtIndex);
                    alphaDistrictIndex[taz] = districtIndex;
                    districtIndex++;
                }
                
            }

            districtNames = new String[districtIndex];
            
            Set keys = districtNameIndex.keySet();
            Iterator it = keys.iterator();
            while ( it.hasNext() ) {
                String key = (String)it.next();
                int index = (Integer)districtNameIndex.get(key);
                districtNames[index] = key;
            }
            
        }
        catch (IOException e) {
            logger.fatal( String.format("exception caught reading alpha/beta zone correspondence file: %s", zoneIndexFile), e);
        }

    }

    
    
    

    private int getMaxCentroid( String zoneIndexFile ) {
        
        // get the filename for the alpha/beta zone correspondence file and 
        // create an object for defining the extent of the study area
        AlphaToBeta a2b = new AlphaToBeta(new File(zoneIndexFile));
        int maxAlphaZone = a2b.getMaxAlphaZone();
        
        int[] externals = getExternalZoneLabels();
        int maxExternalZone = 0;
        for (int i=0; i < externals.length; i++)
            if ( externals[i] > maxExternalZone)
                maxExternalZone = externals[i];
        
        int maxCentroid = Math.max(maxAlphaZone, maxExternalZone);
        
        return maxCentroid;
        
    }

}
