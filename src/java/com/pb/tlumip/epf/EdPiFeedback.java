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
package com.pb.tlumip.epf;


import com.pb.models.reference.ModelComponent;
import com.pb.common.datafile.TableDataSetLoader;
import com.pb.common.datafile.TableDataSet;
import com.pb.common.datafile.CSVFileWriter;

import java.util.ArrayList;
import java.util.ResourceBundle;
import java.util.HashMap;
import java.io.IOException;
import java.io.File;

import org.apache.log4j.Logger;

/**
 * EdPiFeedback is a class that ...
 *
 * @author Christi Willison
 * @version 1.0,  May 27, 2009
 */
public class EdPiFeedback extends ModelComponent {
    protected static Logger logger = Logger.getLogger(EdPiFeedback.class);
    int timeInterval;

    TableDataSet actSumRefTable;
    TableDataSet edPiFeedParams;
    TableDataSet edPiInflFactors;
    TableDataSet piTMinus1ActSummary;
    TableDataSet piTMinus2ActSummary;

    TableDataSet activityDollarDataForPi;
    TableDataSet jobDataForSpg;
    TableDataSet constructionDollarDataForAld;

    HashMap<String, Double> lCalculationsMap = new HashMap <String, Double>();
    HashMap<String, Float> spgPIFCalculations = new HashMap <String, Float>();
    HashMap<String, Float> piPIFCalculations = new HashMap <String, Float>();
    HashMap<String, Float> aldPIFCalculations = new HashMap <String, Float>();


    public EdPiFeedback(ResourceBundle appRb){
        super();
        setApplicationResourceBundle(appRb);
    }

    public void startModel(int baseYear, int timeInterval){
        this.timeInterval = timeInterval;

        logger.info("Reading in the ActivitySummaryReferenceFile");
        actSumRefTable = TableDataSetLoader.loadTableDataSet(appRb, "epf.activity.summary.reference");

        //Read in the ED-PI Feedback input files -
        //ED_PIFeedbackParametersI.csv
        //and ED_PIInfluenceFactors.csv
        logger.info("Reading in the ED_PIFeedbackParametersI.csv");
        edPiFeedParams = TableDataSetLoader.loadTableDataSet(appRb, "epf.feedback.parameters");

        //This method will involve creating some mappings
        //between the PI industries and the SPG and ALD industries
        logger.info("Creating SPG and ALD mappings to PI Industries");
        HashMap<String, ArrayList> spgToPiIndustries = getSpgMapping();
        HashMap<String, ArrayList> aldToPiIndustries = getAldMapping();

        logger.info("Reading in the ED_PIInfluenceFactors.csv");
        edPiInflFactors = TableDataSetLoader.loadTableDataSet(appRb, "epf.influence.factors");

        //Read in the ActivitySummary file for timeInterval-1
        //and timeInterval-2 and store both the size terms and
        //the composite utilities in a HashMap.
        logger.info("Reading in the ActivitySummary file for timeInterval-1");
        piTMinus1ActSummary = TableDataSetLoader.loadTableDataSet(appRb, "epf.ActivitySummary");

        logger.info("Reading in the ActivitySummary file for timeInterval-2");
        piTMinus2ActSummary = TableDataSetLoader.loadTableDataSet(appRb, "epf.PreviousActivitySummary");

        //For each PI Industry in the ED_PIFeedbackParametersI file,
        //calculate L and store in HashMap.
        logger.info("Calculating L values for each PI industry");
        calculateL();
        System.out.println("Agriculture and Mining-Agriculture L: " + lCalculationsMap.get("AGRICULTURE AND MINING-Agriculture"));


        //Read in the 3 ED output files - ActivityDollarDataForPi
        //JobDataForSPG1 and ConstructionDollarDataForALD for the
        //current time interval
        logger.info("Reading in the ActivityDollarDataForPi file for timeInterval");
        activityDollarDataForPi = TableDataSetLoader.loadTableDataSet(appRb, "epf.activityDollarDataForPi");
        calculatePIFsForPIIndustries();
        System.out.println("Agriculture and Mining-Agriculture PIF: " + piPIFCalculations.get("AGRICULTURE AND MINING-Agriculture"));

        logger.info("Reading in the JobDataForSpg file for timeInterval");
        jobDataForSpg = TableDataSetLoader.loadTableDataSet(appRb, "epf.jobDataForSpg");
        calculatePIFsForSPGIndustries(spgToPiIndustries);
        System.out.println("Lower Education PIF: " + spgPIFCalculations.get("LOWER EDUCATION"));


        logger.info("Reading in the ConstructionDollarDataForAld file for timeInterval");
        constructionDollarDataForAld = TableDataSetLoader.loadTableDataSet(appRb, "epf.constructionDollarDataForAld");
        calculatePIFsForALDIndustries(aldToPiIndustries);
        System.out.println("Residential Construction PIF: " + aldPIFCalculations.get("Residential Construction"));
        System.out.println("Non-Residential Construction PIF: " + aldPIFCalculations.get("Non-residential Construction"));
        
        //Once PIF calcs are done for PI, SPG and ALD I need to update the table so that it can
        //be used for the final calculations.  It also needs to be written out to disk.
        updatePIFTable();

        //Build index on the PIF table so that we can
        //pull values to make the final calculations
        edPiInflFactors.buildIndex(1);

        //For each of the output files, loop thru the industries
        //and calculate the new values.  Store in TableDataSet and
        //write to CSV at the end.
        HashMap finalPiCalcs = getFinalCalculation("PI", activityDollarDataForPi);
        writeOutUpdatedValues("PI", finalPiCalcs, activityDollarDataForPi, "Factor");

        HashMap finalSpgCalcs = getFinalCalculation("SPG", jobDataForSpg);
        writeOutUpdatedValues("SPG", finalSpgCalcs, jobDataForSpg, "Employment");

        HashMap finalAldCalcs = getFinalCalculation("ALD", constructionDollarDataForAld);
        writeOutUpdatedValues("ALD", finalAldCalcs, constructionDollarDataForAld, "TotalDollars");



    }

    //For each PI Industry in the ED_PIFeedbackParametersI file,
    //calculate L and store in HashMap.
    private void calculateL(){
        double previousC;      //utility of timeInteval - 1 for pi Activity from ActivitySummary
        double previousPreviousC;   //utility of timeInteval - 2 for pi Activity from PreviousActivitySummary
        double previousRefC;        //utility of timeInteval - 1 for pi Activity from Reference Scenario
        double previousPreviousRefC;      //utility of timeInteval - 2 for pi Activity from Reference Scenario

        for(String piActivity: edPiFeedParams.getColumnAsString("PI_Activity")){
            previousC = getStringIndexedValue(piActivity, "Activity", piTMinus1ActSummary, "CompositeUtility");
            previousPreviousC = getStringIndexedValue(piActivity, "Activity", piTMinus2ActSummary, "CompositeUtility");
            previousRefC = getStringIndexedValue(piActivity, "Activity", actSumRefTable, ""+ (timeInterval-1)+"_CompositeUtility");
            previousPreviousRefC = getStringIndexedValue(piActivity, "Activity", actSumRefTable, ""+ (timeInterval-2)+"_CompositeUtility");

            double lValue = ((previousC - previousPreviousC) - (previousRefC - previousPreviousRefC));
            lCalculationsMap.put(piActivity, lValue);

        }
    }

    private double getStringIndexedValue(String actName, String colToFindActNameIn, 
    									TableDataSet table, String colToGetDataFrom){
        //To get the value in the colToGetDataFrom, first loop through the elements 
    	//in the colToFindActNameIn column in search of the specific actName,
    	int rowNumber = -1;
    	String[] activityNames = table.getColumnAsString(colToFindActNameIn);
    	for(int i = 0; i < activityNames.length; i++){
    		if( activityNames[i].equals(actName)){
    			rowNumber = i;
    			break;
    		}
    	}
    		
    	//get the column called colToGetDataFrom from the table
    	//from the array get element[rowNumber]
    	double[] activityValue = table.getColumnAsDouble(colToGetDataFrom);
        return activityValue[rowNumber];    
    }

    private HashMap<String, ArrayList> getSpgMapping(){
    	//To get the SPG mapping and office share percentages
    	//create an array of the SPG_Industry names so it can be indexed and looped through.
    	HashMap<String, ArrayList> spgMapping = new HashMap<String, ArrayList>();
    	
    	String[] spgIndustryNames = edPiFeedParams.getColumnAsString("SPG_industry");
    	double[] officeSharePercent = edPiFeedParams.getColumnAsDouble("SPG_officeShare");
    	String[] piActivityNames = edPiFeedParams.getColumnAsString("PI_Activity");

    	for( int i = 0; i < spgIndustryNames.length; i++){
    		if(!spgIndustryNames[i].equals("NA")){
    			ArrayList industryAndOfficePercentList = new ArrayList();
    			
    			if (officeSharePercent[i]== 0){     
    				industryAndOfficePercentList.add(piActivityNames[i]);
    				industryAndOfficePercentList.add(0.00);
    				spgMapping.put(spgIndustryNames[i], industryAndOfficePercentList);
                }else if (officeSharePercent[i] > 0){
                	int hyph = piActivityNames[i].indexOf("-");
                	industryAndOfficePercentList.add(piActivityNames[i]);
                	industryAndOfficePercentList.add("" + piActivityNames[i].substring(0, hyph) + "-Office" );
    				industryAndOfficePercentList.add(officeSharePercent[i]);
                	spgMapping.put(spgIndustryNames[i], industryAndOfficePercentList);
                }

    		}
    	}

 	 
    	return spgMapping;                   
	 }
    
    private HashMap<String, ArrayList> getAldMapping(){
    	HashMap<String, ArrayList> aldMapping = new HashMap<String, ArrayList>();
    	String[] piActivityNames = edPiFeedParams.getColumnAsString("PI_Activity");
    	String[] aldActivityNames = edPiFeedParams.getColumnAsString("ALD_category");
    	
		ArrayList nonResList = new ArrayList();
		ArrayList resList = new ArrayList();
		
    	for(int i = 0; i < aldActivityNames.length; i++){
    		if (aldActivityNames[i].equals("NRES")){
    			nonResList.add(piActivityNames[i]);
    			
    		}else if (aldActivityNames[i].equals("RES")){
    			resList.add(piActivityNames[i]);
    		}
    	}
    	aldMapping.put("Non-residential Construction", nonResList);
    	aldMapping.put("Residential Construction", resList);
    	return aldMapping;
    }
    

    private void calculatePIFsForSPGIndustries (HashMap<String, ArrayList> spgToPiMapping){
        //loop thru each SPG industry and calculate the PIF value.
        for (String spgIndustry : spgToPiMapping.keySet() ){
            System.out.println("SPG Industry: " + spgIndustry);
            ArrayList piActivitiesAndOfficePercent = spgToPiMapping.get(spgIndustry);
            //get parameters and L values for each industry in the arraylist.
            //For the PIF calculation we need delta, mu, sizes,  eta, and L
            int numPiIndustries = piActivitiesAndOfficePercent.size()-1;
            double[] deltas = new double[numPiIndustries];
            double[] mus = new double[numPiIndustries];
            double[] sizes = new double[numPiIndustries];
            double[] ls = new double[numPiIndustries];
            double[] etas = new double[numPiIndustries];

            double officePercent = (Double) piActivitiesAndOfficePercent.get(numPiIndustries);

            //get the delta, mu, size and l value.
            int i = 0;
            String piActivityName;
            for (Object aPiActivitiesAndOfficePercent : piActivitiesAndOfficePercent) {
                //iterate thru the industries, a cast exception will
                //be thrown when the end of the industries are reached.
                try {
                    piActivityName = (String) aPiActivitiesAndOfficePercent;
                    deltas[i] = getStringIndexedValue(piActivityName, "PI_Activity", edPiFeedParams, "Delta Exp Term Coeff");
                    mus[i] = getStringIndexedValue(piActivityName, "PI_Activity", edPiFeedParams, "Mu Linear Term Coeff");
                    sizes[i] = getStringIndexedValue(piActivityName, "Activity", piTMinus1ActSummary, "Size");
                    ls[i] = lCalculationsMap.get(piActivityName);
                    etas[i] = getStringIndexedValue(piActivityName, "PI_Activity", edPiFeedParams, "Eta Utility Scaling");
//                    System.out.println("Pi Industry: " + piActivityName);
//                    System.out.println(" deltas: " + deltas[i]);
//                    System.out.println(" mus[i]: " + mus[i]);
//                    System.out.println(" sizes[i]: " + sizes[i]);
//                    System.out.println(" ls[i]: " + ls[i]);
                    i++;

                } catch (Exception e) {
                    //end of industries has been reached, the last entry in
                    //array is the officePercent value so loop should exit
                }
            }

            //adjust the office size by the office percent if the office percent is greater than 0
            if(numPiIndustries > 1){
                //true implies the spg industry is the aggregation of the piIndustry and the piIndustry-Office.
                //so adjust the size term of the office industry by the office percent.
                sizes[sizes.length-1] *= officePercent;
            }
            double denominator = 0.0;
            for(double size : sizes){
                denominator += size;
            }

            //calculate the weight-averaged pif.  The size term is used for the weighting.
            double pif = 0.00;
            for(i=0; i < numPiIndustries; i++){
                pif += sizes[i] * calculateA(deltas[i], ls[i], mus[i], etas[i]);
            }

            spgPIFCalculations.put(spgIndustry, (float) (pif/denominator));
//            System.out.println("PIF for Lower Education: " + spgPIFCalculations.get("LOWER EDUCATION"));
//            System.out.println("PIF for Food Products - Heavy Industry: " + spgPIFCalculations.get("FOOD PRODUCTS-Heavy Industry"));
//            System.out.println("PIF for Food Products - Light Industry: " + spgPIFCalculations.get("FOOD PRODUCTS-Light Industry"));
        }
    }

    private void calculatePIFsForPIIndustries (){
        //loop thru each Pi industry and calculate the PIF value.
        double delta;
        double mu;
        double l;
        double eta;
        for (String piIndustry : activityDollarDataForPi.getColumnAsString("Activity")){
            System.out.println("Pi Industry: " + piIndustry);
            try {
                delta = getStringIndexedValue(piIndustry, "PI_Activity", edPiFeedParams, "Delta Exp Term Coeff");
                mu = getStringIndexedValue(piIndustry, "PI_Activity", edPiFeedParams, "Mu Linear Term Coeff");
                l = lCalculationsMap.get(piIndustry);
                eta = getStringIndexedValue(piIndustry, "PI_Activity", edPiFeedParams, "Eta Utility Scaling");
                System.out.println("\tDelta: " + delta);
                System.out.println("\tMu " + mu);
                System.out.println("\tl " + l);

                //calculate the pif.  No weighting for Pi calculations
                //since there is a 1-1 mapping.
                double pif = calculateA(delta, l, mu, eta);
                System.out.println("\tPIF: " + pif);
                piPIFCalculations.put(piIndustry, (float) pif);

            } catch (Exception e) { //these 2 piIndustries are not included in these calculations.
                if(!piIndustry.equals("Capitalists") && !piIndustry.equals("GovInstitutions"))
                    throw new RuntimeException("Couldn't find delta, mu or l values for " + piIndustry);
            }


        }
    }

    private void calculatePIFsForALDIndustries (HashMap<String, ArrayList> aldToPiMapping){
            //loop thru each ALD industry and calculate the PIF value.
            for (String aldIndustry : aldToPiMapping.keySet() ){
                ArrayList piActivitiesList = aldToPiMapping.get(aldIndustry);
                //get parameters and L values for each industry in the arraylist.
                //For the PIF calculation we need delta, mu, sizes, hh unit conversion factor, and L
                int numPiIndustries = piActivitiesList.size();
                double[] deltas = new double[numPiIndustries];
                double[] mus = new double[numPiIndustries];
                double[] ls = new double[numPiIndustries];
                double[] sizes = new double[numPiIndustries];
                double[] etas = new double[numPiIndustries];
                int hh = Integer.parseInt( appRb.getString( "epf.hh.units.conversion.factor") );

                //get the delta, mu, size, eta, l, and hh unit conversion factor value.
                int i = 0;
                String piActivityName;
                for (Object aPiActivityName : piActivitiesList) {
                    piActivityName = (String) aPiActivityName;
                    deltas[i] = getStringIndexedValue(piActivityName, "PI_Activity", edPiFeedParams, "Delta Exp Term Coeff");
                    sizes[i] = getStringIndexedValue(piActivityName, "Activity", piTMinus1ActSummary, "Size");
                    mus[i] = getStringIndexedValue(piActivityName, "PI_Activity", edPiFeedParams, "Mu Linear Term Coeff");
                    ls[i] = lCalculationsMap.get(piActivityName);
                    etas[i] = getStringIndexedValue(piActivityName, "PI_Activity", edPiFeedParams, "Eta Utility Scaling");
                    i++;

                }
                
                double denominator = 0.0;
                for(double size : sizes){
                    denominator += size;
                }
                
                double pif = 0.00;
                for(i=0; i < numPiIndustries; i++){
                	if (aldIndustry.equals("Non-residential Construction")){
                		pif += sizes[i] * calculateA(deltas[i], ls[i], mus[i], etas[i]);
                	} else{
                		pif += sizes[i] * calculateA(deltas[i], ls[i] / hh, mus[i], etas[i]);
                	}                  
                	
                }
               
                aldPIFCalculations.put(aldIndustry, (float)(pif/denominator));
            }
        }

    private double calculateA(double delta, double l, double mu, double eta){
            return (1.0 + delta * ((1.0 - Math.exp(l*eta)) / (1.0 + Math.exp(l*eta))) + mu * l);
    }
    
    private HashMap<String, Float> getFinalCalculation(String module, TableDataSet table){
    	HashMap<String, Float> finalCalculation = new HashMap<String, Float>();
        String columnName = "";
        String valueColumnName = "";
        if(module.equals("PI")){
            columnName = "Activity";
            valueColumnName = "Factor";
        }else if(module.equals("SPG")){
            columnName = "SPG Sector";
            valueColumnName = "Employment";
        }else if(module.equals("ALD")){
            columnName = "ConstructionType";
            valueColumnName = "TotalDollars";
        }

        String[] activityNames = table.getColumnAsString(columnName);
    	

       	for(String actName : activityNames ){
               //This is for PI but it doesn't hurt for SPG or ALD
            if(actName.equals("Capitalists") || actName.equals("GovInstitutions"))
                continue;
       		double calcResult;
       		double pifValue;
       		calcResult = getStringIndexedValue(actName, columnName, table, valueColumnName);
       		   String newActName = null;
               for (int t = timeInterval; t >= 1; t--){
       			newActName = module + "_" + actName;
       			pifValue = edPiInflFactors.getIndexedValueAt(t, newActName);
       			calcResult *= pifValue;
       		}
       		
       		finalCalculation.put(newActName, (float) calcResult);

        }
        return finalCalculation;
    }


    //This method assumes that the first 2 columns in the PIF file store
    //the timeInterval and the timeInterval - 1 values.  After
    //that this method is not sensitive to the column order.
    private void updatePIFTable(){
        //make a HashMap of new Row Data
        HashMap newData = new HashMap();
        String[] headers = edPiInflFactors.getColumnLabels();
        for(String header : headers){
            System.out.println("Header: " + header);
            if(header.equals("1st Year Applied (i)")){
                newData.put(header, (float) timeInterval);
            }else if(header.equals("PI Data Year  (i-1)")){
                newData.put(header, (float) (timeInterval-1));
            }else {
//                System.out.println("Header: " + header);
//                System.out.println("Substring: " + header.substring(3));
                if(header.startsWith("PI")){
                    newData.put(header, piPIFCalculations.get(header.substring(3)));
                }else if(header.startsWith("SPG")){
                    newData.put(header, spgPIFCalculations.get(header.substring(4)));
                }else
                    newData.put(header, aldPIFCalculations.get(header.substring(4)));
            }

        }
        edPiInflFactors.appendRow(newData);
        CSVFileWriter fileWriter = new CSVFileWriter();
        try {
            fileWriter.writeFile(edPiInflFactors, new File(appRb.getString("epf.current.influence.factors")));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void writeOutUpdatedValues(String module, HashMap finalCalculations,
                                      TableDataSet originalTable, String newColLabel){
        String outputPath = appRb.getString(module.toLowerCase() + ".working.file");
        String[] actNames = originalTable.getColumnAsString(1);
        float[] newValues = new float[actNames.length];

        //build up an array of new values that i will
        //append to the table
        int index = 0;
        for(String actName : actNames){
            if(actName.equals("Capitalists") || actName.equals("GovInstitutions")){
                newValues[index] = originalTable.getColumnAsFloat(2)[index];
            }else {
                newValues[index] = (Float) finalCalculations.get(module + "_" + actName);
            }
            index++;
        }
        String[] oldColLabels = originalTable.getColumnLabels();
        String[] newLabels = new String[oldColLabels.length];
        int i = 0;
        for (String oldLabels : oldColLabels){
        	if (oldLabels.equals(newColLabel)){
        		newLabels[i] = "Old"+ oldLabels;
        	}else{
        		newLabels[i] = oldLabels;
        	}
        	i++;
        }

        originalTable.setColumnLabels(newLabels);
        originalTable.appendColumn(newValues, newColLabel);
        CSVFileWriter fileWriter = new CSVFileWriter();
        try {
            fileWriter.writeFile(originalTable, new File(outputPath));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

}
