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
/* Generated by Together */

package com.pb.tlumip.ha;
import com.pb.common.util.ResourceUtil;

import java.util.ResourceBundle;


public class WritePopulationAndGridCellsToTextFiles {
    public static void main(String[] args) {
        ResourceBundle rb = ResourceUtil.getResourceBundle( "ha" );
        String path = ResourceUtil.getProperty(rb, "outputPopulation.path");
        HAModel.dm.exportTable("SynPopH", path+"SynPopH");
        HAModel.dm.exportTable("SynPopP", path+"SynPopP");
        path = ResourceUtil.getProperty(rb, "outputGridcells.path");
        HAModel.dm.exportTable("GridCells", path+"GridCells");
        System.exit(0);
    }
}
