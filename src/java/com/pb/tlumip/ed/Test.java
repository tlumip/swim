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
 * Title:        null<p>
 * Description:  null<p>
 * Copyright:    null<p>
 * Company:      ECONorthwest<p>
 * @author
 * @version null
 */
package com.pb.tlumip.ed;

import java.io.File;

public class Test {

final static int CURRENTYEAR = 1998;
//final static String DATALOCATION = "temp2.csv";
final static String DATALOCATION = "model_data.csv";
//final static String XMLLOCATION = "EDtest.xml";
//final static String XMLLOCATION = "test.xml";
final static String XMLLOCATION = "model.xml";
final static String MYSQLSESSION = "jdbc:mysql://localhost/EDModelTest?user=abe=abe";
final static String MYSQLLOGIN = "abe";
//static final String home = "/home/abe/p652Sandbox2Duplicate/runtime/classes/";
//static final String home = "/home/abe/p652Sandbox062800/sandbox/development/data/";
static final String home= "C:\\eclipse\\workspace\\tlumip\\";
  public static void main(String args[]) {
 	System.out.println("Starting test...");
 	if (args.length > 1)
 	{
 		try
 		{
 			// Year TestYear propertiesFile
 			EDControl e = new EDControl(new Integer(args[0]).intValue(), new Integer(args[1]).intValue(), args[2]);
			e.startModel();
		  } catch(Exception ex) {
			ex.printStackTrace();
		  }
 	}
    test5();
    //testfunf();
  }

  public static void test5() {
    try {
      //EDControl e = new EDControl(CURRENTYEAR,DATALOCATION,home + XMLLOCATION, true);
      EDControl e = new EDControl(CURRENTYEAR, 0, "C:/eclipse/workspace/tlumip/config/ed/ed.properties");
      e.startModel();
    } catch(Exception ex) {
      ex.printStackTrace();
    }

  }
  
  public static void testfunf()
  {
  	try
  	{
		CSVSplitter.split(new File("infli.csv"), new File("outfli.csv"), new File("split.csv"), 2003);
  	}catch (Exception e)
  	{
  		e.printStackTrace();
  	}
  }
/*
  public static void test4() {
    XMLModelCreator xml = new XMLModelCreator("/home/ftp/pub/project652Sandbox2/development/tlumip/javasrc/com/pb/tlumip/ed/EDModelXML.dtd", "/home/ftp/pub/project652Sandbox2/development/tlumip/javasrc/com/pb/tlumip/ed/jakjh.xml");
    xml.addSubModel();
    xml.subModelName("fred");
    xml.writeOutXML();
  }

  public static void test2() {
    EDControl e = new EDControl(2000,"my house","/home/ftp/pub/project652Sandbox2/development/tlumip/javasrc/com/pb/tlumip/ed/EDModelXML.xml", true);

  }
*/
}
