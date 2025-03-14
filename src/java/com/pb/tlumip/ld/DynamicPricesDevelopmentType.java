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

import com.pb.models.pecas.AbstractZone;

public class DynamicPricesDevelopmentType extends DevelopmentType {
    private double portionVacantMultiplier;
    private double equilibriumVacancyRate;
    private double minimumBasePrice;

    public DynamicPricesDevelopmentType(String name, double portionVacantMultiplier, double equilibriumVacancyRate, double minimumBasePrice, int gridCode) {
        super(name, gridCode);
        this.portionVacantMultiplier = portionVacantMultiplier;
        this.minimumBasePrice = minimumBasePrice;
        this.equilibriumVacancyRate = equilibriumVacancyRate;
    }

    public void updatePrices(double elapsedTime) {
        AbstractZone[] zones = AbstractZone.getAllZones();
        for (int z = 0; z < zones.length; z++) {
            AbstractZone.PriceVacancy pv = zones[z].getPriceVacancySize(this);
            if (pv.getTotalSize() > 0) {
                double price = pv.getPrice();
                if (price < minimumBasePrice) price = minimumBasePrice;
                double extraVacancy = pv.getVacancy() / pv.getTotalSize() - equilibriumVacancyRate;
                double increment = price * extraVacancy * portionVacantMultiplier;
                double newPrice = pv.getPrice() - increment;
                if (newPrice < 0) newPrice = 0;
                zones[z].updatePrice(this, newPrice);
            }
        }
    }

    public String toString() { return "DevelopmentType " + name; };
}

