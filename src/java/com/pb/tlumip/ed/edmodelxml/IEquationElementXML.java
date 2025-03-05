/**
 * IEquationElementXML.java	Java 1.4.2_07 Tue Jul 19 13:08:04 MDT 2005
 *
 * Copyright 1999 by ObjectSpace, Inc.,
 * 14850 Quorum Dr., Dallas, TX, 75240 U.S.A.
 * All rights reserved.
 * 
 * This software is the confidential and proprietary information
 * of ObjectSpace, Inc. ("Confidential Information").  You
 * shall not disclose such Confidential Information and shall use
 * it only in accordance with the terms of the license agreement
 * you entered into with ObjectSpace.
 */

package com.pb.tlumip.ed.edmodelxml;

import java.util.Hashtable;

public interface IEquationElementXML extends com.objectspace.xml.IDXMLInterface ,com.objectspace.xml.IAttributeContainer
  {

  // element Attributes
  public String getTypeAttribute();
  public void setTypeAttribute( String value );
  public String removeTypeAttribute();

  // element VariableXML
  public IVariableXML getVariableXML();
  public void setVariableXML( IVariableXML arg0 );

  // element ParameterXML
  public String getParameterXML();
  public void setParameterXML( String arg0 );

  // element OperatorXML
  public String getOperatorXML();
  public void setOperatorXML( String arg0 );
  }