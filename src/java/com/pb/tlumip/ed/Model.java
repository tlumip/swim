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

/**
 * The model class contains and starts all the submodels.
 */
package com.pb.tlumip.ed;

import com.pb.models.utils.StatusLogger;
import org.apache.log4j.Logger;

import java.util.Hashtable;
import java.util.Vector;


public class Model {
  protected static Logger logger = Logger.getLogger(Model.class);
  protected static Logger statusLogger = Logger.getLogger("status");
  private Vector submodels;
  private Hashtable errors;
  private boolean hasErrors;

  /**
   * This constructor takes a vector of SubModels as input, and arranges the
   * order they are run based on the SubModel order attribute.
   */
  public Model(Vector sms) {
    submodels = new Vector();
    SubModel tempSM;
    for(int i=0; i<sms.size() ; i++) {
      tempSM = (SubModel) sms.get(i);
      addSubModel(tempSM);
    }
  }

  /**
   * Adds a SubModel to the vector of SubModels in the order specified by
   * the SubModels order attribute.
   */
  public void addSubModel(SubModel sm) {
    int beginSize = submodels.size();
    boolean addedSubModel=false;
    for(int i=0; i<submodels.size(); i++) {
        if(sm.getOrder() < ((SubModel)submodels.get(i)).getOrder()) {
          submodels.add(i,sm);
          addedSubModel = true;
        }
    }
    if (!addedSubModel) {
      submodels.add(sm);
    }
  }




  /**
   *Retrieves data for each submodel, solves the submodel, and writes output
   * to the database.  Errors are placed in a Hashtable and may be looked at
   * when the model finishes running.
   */
  protected void start() {
    SubModel s;
    hasErrors = false;
    errors = new Hashtable();
    int max = submodels.size();
    for(int i=0 ; i<max; i++) {
      try {
        s =(SubModel)submodels.get(i);
        logger.info("Beginning submodel '" + s.getName()+ "' of type '" + s.getType() + "'");
        logger.info("\tReading data");
        s.getData();
        logger.info("\tSolving model");
        s.solve();
        logger.info("\tWriting data");
        s.setData();
        StatusLogger.logText("ed",s.getName() + " finished");

      } catch(Exception e) {
        hasErrors = true;
        logger.error(e.toString());
        SubModel errorSubModel = (SubModel) submodels.get(i);
        if(errorSubModel.getName() ==null){
          errors.put(String.valueOf(errorSubModel.getOrder()), e);
        }else{
          errors.put(errorSubModel.getName(), e);
        }
      }
    }
  }

  /**
   * This method returns a hashtable of errors for the last running of this
   * model.
   */
  protected Hashtable getErrors() {
    return errors;
  }

  /**
   * Returns true if there was an error in the last run of the model.
   */
  protected boolean hasErrors() {
    return hasErrors;
  }

  /**
   * This method returns the number of submodels in this model.
   */
  protected int numberSubModels() {
    return submodels.size();
  }

}
