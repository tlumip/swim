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
package com.pb.tlumip.ld;

import com.pb.common.datafile.TableDataSet;
import com.pb.models.pecas.AbstractZone;
import com.pb.models.pecas.DevelopmentTypeInterface;
import com.pb.models.pecas.GridCell;
import com.pb.models.reference.ModelComponent;

/**
 * SDModel implements...
 * @author    John Abraham
 * @version   1.0, 3/31/2000
 */
public class LDModel extends ModelComponent {
    public static void setUpGridCells(TableDataSet gtab) {
        for(int r=1;r<=gtab.getRowCount();r++) {
            int zoneNumber = (int)gtab.getValueAt(r,"TAZ");
            AbstractZone zone = AbstractZone.findZoneByUserNumber(zoneNumber);
            if (zone == null) {
                throw new Error("Grid cell has undefined zone number " + zoneNumber);
            }
            DevelopmentTypeInterface currentDevelopment =
                DevelopmentType.getAlreadyCreatedDevelopmentType(gtab.getStringValueAt(r,"DevelopmentType"));
                if (currentDevelopment == null)
                    throw new Error("Incorrect Development Type " + gtab.getStringValueAt(r,"DevelopmentType") +
                    " in GridCell.csv");
            float amountDeveloped = gtab.getValueAt(r,"AmountOfDevelopment");
            float amountLand = gtab.getValueAt(r,"AmountOfLand");
            int age = (int)gtab.getValueAt(r,"Age");
            ZoningScheme zs = ZoningScheme.getAlreadyCreatedZoningScheme(gtab.getStringValueAt(r,"ZoningScheme"));
            if (zs == null)
                throw new Error("Incorrect zoning scheme in GridCells.csv: " + gtab.getStringValueAt(r,"ZoningScheme"));
            GridCell gc = new GridCell(zone, amountLand, currentDevelopment, amountDeveloped, age, zs);
        }
    }

    public static void writeUpdatedGridCells(TableDataSet gtab) {
        //TODO replace 'gtab.empty' with an equivalent com.pb.common.TableDataSet method.
//          gtab.empty();
          AbstractZone[] allZones = AbstractZone.getAllZones();
          for (int z=0;z<allZones.length; z++) {
               AbstractZone taz = allZones[z];
               taz.writeGridCells(gtab);
          }
    }


    public void startModel(int baseYear, int timeInterval){};

}
