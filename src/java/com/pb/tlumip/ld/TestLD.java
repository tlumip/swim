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

package com.pb.tlumip.ld;

import com.pb.common.datafile.TableDataSet;
import com.pb.common.datastore.DataManager;
import com.pb.common.util.ResourceUtil;
import com.pb.models.pecas.AbstractZone;
import com.pb.models.pecas.DevelopmentTypeInterface;

import java.util.ArrayList;
import java.util.ResourceBundle;

public class TestLD {
   public static void main(String[] args) {
        ResourceBundle rbld = ResourceUtil.getResourceBundle( "ld" );
        gridPath = ResourceUtil.getProperty(rbld, "grid.path");
        ResourceBundle rbha = ResourceUtil.getResourceBundle( "ha" );
        String spaceTypePath = ResourceUtil.getProperty(rbha, "spaceType.path");
        DevelopmentTypeInterface dtypes[] = setUpDevelopmentTypes(spaceTypePath);
        TableDataSet ztab = reloadTableFromScratchFromTextFile(spaceTypePath,"Zones");
        TAZ.setUpZones(ztab);
        readInSpacePrices(spaceTypePath);


        ZoningScheme.setUpZoningSchemes(reloadTableFromScratchFromTextFile(spaceTypePath,"ZoningSchemes"));
        
        ZoningScheme currentScheme = null;
        GridLandInventory gli = new GridLandInventory();
        gli.openGridFiles(gridPath);
        ZoningScheme.openLogFile(gridPath);
        for (long row = 0; row < gli.getId1Extent();row  ++) {
            for (long col=0;col<gli.getId2Extent(row);col++ ) {
                if (gli.isDevelopable(row,col)) {
                    currentScheme = ZoningScheme.getZoningSchemeByIndex(gli.getZoning(row,col));
                    currentScheme.doDevelopment(gli, row,col);
                }
            }
            System.out.println("row "+row);
            ZoningScheme.flushDevelopmentLog();
        }
        ZoningScheme.closeLogFile();
        gli.closeGridFiles();
        System.exit(0);
        
    }

    private static TableDataSet reloadTableFromScratchFromTextFile(String path, String tableName) {
        //TODO This method needs to be rewritten to avoid using the Borland Table Data Set
        //TODO  and use the common.datafile.tabledata set instead.  In order to avoid compile errors I am commenting out the method and returning a dummy TableDataSet.
        //TODO DO NOT USE THIS METHOD UNTIL JOHN HAS FIXED THE CODE.
//        TableDataSet table = dm.getTableDataSet(tableName);
//        try {
//            table.empty();
//        } catch (com.borland.dx.dataset.DataSetException e) { };
//        DataManager.closeTable(table);
//        dm.deleteTable(tableName);
//        dm.loadTable(tableName, path + tableName, path + tableName);
//        table = dm.getTableDataSet(tableName); //Add a table to data-store
//        return table;
        return new TableDataSet();
    }

    static public final DataManager dm = new DataManager();
    static private String gridPath;

    public static DevelopmentType[] setUpDevelopmentTypes(String spaceTypePath) {
        TableDataSet ctab = reloadTableFromScratchFromTextFile(spaceTypePath,"DevelopmentTypes");
        ArrayList dtypes = new ArrayList();
        for(int r=1;r<=ctab.getRowCount();r++) {
            String typeName = (ctab.getStringValueAt(r, "DevelopmentTypeName"));
            boolean dynamic = (Boolean.valueOf(ctab.getStringValueAt(r,"DynamicPricesDevelopmentType"))).booleanValue();
            int gridCode = ctab.getStringValueAt(r,"GridCode").charAt(0);
            DevelopmentTypeInterface newDevelopmentType;
            if (dynamic) {
                newDevelopmentType = new DynamicPricesDevelopmentType(typeName,
                    (double)ctab.getValueAt(r,"PortionVacantMultiplier"),
                    (double)ctab.getValueAt(r,"EquilibriumVacancyRate"),
                    (double)ctab.getValueAt(r,"MinimumBasePrice"),gridCode);
            } else {
                newDevelopmentType = new DevelopmentType(typeName,gridCode);
            }
            newDevelopmentType.setConstructionCost((double)ctab.getValueAt(r,"ConstructionCost"));
            dtypes.add(newDevelopmentType);
        }
        DevelopmentType[] d = new DevelopmentType[dtypes.size()];
        return (DevelopmentType[]) dtypes.toArray(d);
    }

    public static void readInSpacePrices(String spaceTypePath) {
        TableDataSet tab = reloadTableFromScratchFromTextFile(spaceTypePath,"FloorspaceRents");
        for(int r=1;r<=tab.getRowCount();r++) {
            String typeName = tab.getStringValueAt(r,"DevelopmentType");
            DevelopmentTypeInterface dt = DevelopmentType.getAlreadyCreatedDevelopmentType(typeName);
            int zone = (int)tab.getValueAt(r,"Zone");
            double price = (double)tab.getValueAt(r,"Price");
            AbstractZone taz = AbstractZone.findZoneByUserNumber(zone);
            if (taz == null) {
                System.out.println("ERROR: unknown zone number in FloorspaceRents.csv "+zone);
            }  else taz.updatePrice(dt,price);
        }
    }
    


}

