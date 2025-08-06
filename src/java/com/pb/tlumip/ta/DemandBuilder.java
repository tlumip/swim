package com.pb.tlumip.ta;

import com.pb.common.matrix.Matrix;
import com.pb.common.matrix.ZipMatrixWriter;
import com.pb.common.matrix.ZipMatrixReader;
import com.pb.common.util.ResourceUtil;
import com.pb.models.pt.TripModeType;
import com.pb.models.pt.ldt.LDTripModeType;

import java.io.*;
import java.nio.file.Paths;
import java.util.*;

/**
 * The {@code DemandBuilder} ...
 *
 * @author crf
 *         Started 1/27/14 3:18 PM
 */
public class DemandBuilder {
    public static final String DEMAND_OUTPUT_MODE_STRING = "{MODE}";
    public static final String DEMAND_OUTPUT_TIME_PERIOD_STRING = "{PERIOD}";
    public static final String DEMAND_OUTPUT_FILE_NAME_PROPERTY = "demand.output.filename";
    public static final String ALPHA_BETA_FILE_PROPERTY = "alpha2beta.file";
    public static final String EXTERNAL_STATIONS_PROPERTY = "external.stations";


    public static final String SDT_TRIP_FILE_PROPERTY = "sdt.person.trips";
    public static final String LDT_TRIP_FILE_PROPERTY = "ldt.vehicle.trips";
    public static final String CT_TRIP_FILE_PROPERTY = "ct.truck.trips";
    public static final String ET_TRIP_FILE_PROPERTY = "et.truck.trips";

    public static final String CT_TRIP_SKIM_PROPERTY = "ct.travel.time.skims";
    public static final String PT_TRIP_SKIM_PROPERTY = "pt.car.peak.skims";

    public static final double AVERAGE_SR3P_AUTO_OCCUPANCY = 3.33f;

    private final ResourceBundle resourceBundle;

    public DemandBuilder(ResourceBundle rb) {
        resourceBundle = rb;
    }

    public void writeDemandMatrices() {
        Map<Integer,Integer> zonesIndexMap = buildZoneIndex();
        writeDemandMatrices(buildDemandMatrices(zonesIndexMap),zonesIndexMap);
    }

    private Map<AssignMode,Map<TimePeriod,double[][]>> buildDemandMatrices(Map<Integer,Integer> zonesIndexMap) {
        String sdtFile = resourceBundle.getString(SDT_TRIP_FILE_PROPERTY);
        String ldtFile = resourceBundle.getString(LDT_TRIP_FILE_PROPERTY);
        String etFile = resourceBundle.getString(ET_TRIP_FILE_PROPERTY);
        String ctFile = resourceBundle.getString(CT_TRIP_FILE_PROPERTY);

        Map<AssignMode,Map<TimePeriod,double[][]>> trips = new EnumMap<>(AssignMode.class);
        int matrixLength = zonesIndexMap.size();
        for (AssignMode mode : AssignMode.values()) {
            Map<TimePeriod,double[][]> timeTrips = new EnumMap<>(TimePeriod.class);
            for (TimePeriod timePeriod : TimePeriod.values())
                timeTrips.put(timePeriod,new double[matrixLength][matrixLength]);
            trips.put(mode,timeTrips);
        }

        readIndividualTripFile(sdtFile,trips,zonesIndexMap);
        readIndividualTripFile(ldtFile,trips,zonesIndexMap);
        readTruckFile(ctFile,trips,zonesIndexMap);
        readTruckFile(etFile,trips,zonesIndexMap);

        return trips;
    }

    private Map<Integer,Integer> buildZoneIndex() {
        Map<Integer,Integer> zoneIndex = new HashMap<>();
        String line;
        int counter = 0;
        String zoneFile = resourceBundle.getString(ALPHA_BETA_FILE_PROPERTY);
        boolean first = true;
        try (BufferedReader reader = new BufferedReader(new FileReader(zoneFile))) {
            while ((line = reader.readLine()) != null) {
                if (first) {
                    first = false;
                    continue;
                }
                if (line.trim().length() == 0)
                    continue;
                zoneIndex.put(new Double(Double.parseDouble(line.split(",")[0])).intValue(),counter++); //zone is assumed to be in first column
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        for (String externalStation : resourceBundle.getString(EXTERNAL_STATIONS_PROPERTY).trim().split(" "))
            zoneIndex.put(Integer.parseInt(externalStation),counter++);
        return zoneIndex;
    }

    private float[][] readTruckSkims() {
        String ctSkimFile = resourceBundle.getString(CT_TRIP_SKIM_PROPERTY);
        Matrix ctSkimMatrix = new ZipMatrixReader(new File(ctSkimFile)).readMatrix();
        return ctSkimMatrix.getValues();
    }

    private float[][] readAutoSkims() {
    	String ctSkimFile = resourceBundle.getString(CT_TRIP_SKIM_PROPERTY);
        String ptSkimFile = Paths.get(Paths.get(ctSkimFile).getParent().toString(), resourceBundle.getString(PT_TRIP_SKIM_PROPERTY).split(", ")[0]+".zmx").toString();
        Matrix ptSkimMatrix = new ZipMatrixReader(new File(ptSkimFile)).readMatrix();
        return ptSkimMatrix.getValues();
    }

    private TimePeriod[] buildTimePeriodMapping() {
        int timeSize = 2400+1;
        TimePeriod[] timePeriodMapping = new TimePeriod[timeSize];
        for (TimePeriod timePeriod : TimePeriod.values()) {
            String rbPrefix = timePeriod.idPrefix + "." + timePeriod.idSuffix;
            int t = Integer.parseInt(resourceBundle.getString(rbPrefix + ".start"));
            int end = Integer.parseInt(resourceBundle.getString(rbPrefix + ".end")) + 1;
            while (t != end) {
                timePeriodMapping[t] = timePeriod;
                t = (t + 1) % timeSize;
            }
        }
        return timePeriodMapping;
    }

    private int newStartTime(String tripStartTime, int duration) {
    	String[] timeParts =  Double.toString(Double.parseDouble(tripStartTime)/100).split("\\.");
    	int hours = Integer.parseInt(timeParts[0]);
    	int minutes = Integer.parseInt(timeParts[1]);
    	int halfDuration = duration/2;
    	minutes += halfDuration;
    	hours += minutes/60;
    	minutes %= 60;
    	hours %= 24;
    	int updatedStartTime = (hours * 100) + minutes;
        return updatedStartTime;
    }

    private void readIndividualTripFile(String tripFile, Map<AssignMode,Map<TimePeriod,double[][]>> trips, Map<Integer,Integer> zonesIndexMap) {
        TimePeriod[] timePeriodMapping = buildTimePeriodMapping();
        try (BufferedReader reader = new BufferedReader(new FileReader(tripFile))) {
            int origin = -1;
            int dest = -1;
            int time = -1;
            int mode = -1;
            int duration = -1;
            String line;
            float[][] ptSkims = readAutoSkims();
            while ((line = reader.readLine()) != null) {
                String[] data = line.split(",");
                if (origin < 0) {
                    //header
                    for (int i = 0; i < data.length; i++) {
                        String column = data[i];
                        if (column.equalsIgnoreCase("origin"))
                            origin = i;
                        if (column.equalsIgnoreCase("destination"))
                            dest = i;
                        if (column.equalsIgnoreCase("tripStartTime"))
                            time = i;
                        if (column.equalsIgnoreCase("tripMode"))
                            mode = i;
                        if (column.equalsIgnoreCase("time"))
                            duration = i;
                    }
                    continue;
                }
                int o = zonesIndexMap.get(Integer.parseInt(data[origin]));
                int d = zonesIndexMap.get(Integer.parseInt(data[dest]));
                float traveltime = ptSkims[o][d];
                Mode m = Mode.getModeForModeString(data[mode]);
                if (m == null)
                    continue;
                TimePeriod t = timePeriodMapping[newStartTime(data[time], (int) traveltime)];
                switch (m) {
                	case DA: break;
                	case SR2: break;
                	case SR3P: break;
                	case INTERCITY : t = timePeriodMapping[newStartTime(data[time], (int) Double.parseDouble(data[duration]))];
                	case INTRACITY : t = timePeriodMapping[newStartTime(data[time], (int) Double.parseDouble(data[duration]))];          
                }
                double trip = 1;
                switch (m) {
                    case DA : break;
                    case SR2 : trip /= 2.0; break;
                    case SR3P : trip /= AVERAGE_SR3P_AUTO_OCCUPANCY; break;
                    case INTERCITY : break;
                    case INTRACITY : break;
                }
                AssignMode am = m.getAssignMode();
                trips.get(am).get(t)[o][d] += trip;
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }



    private void readTruckFile(String truckFile,Map<AssignMode,Map<TimePeriod,double[][]>> trips, Map<Integer,Integer> zonesIndexMap) {
        TimePeriod[] timePeriodMapping = buildTimePeriodMapping();

        try (BufferedReader reader = new BufferedReader(new FileReader(truckFile))) {
            int origin = -1;
            int dest = -1;
            int time = -1;
            int truckType = -1;
            int trucks = -1;
            int duration = -1;
            String line;
            float[][] ctSkims = readTruckSkims();
            while ((line = reader.readLine()) != null) {
                String[] data = line.split(",");
                if (origin < 0) {
                    //header
                    for (int i = 0; i < data.length; i++) {
                        String column = data[i];
                        if (column.equalsIgnoreCase("origin"))
                            origin = i;
                        if (column.equalsIgnoreCase("destination"))
                            dest = i;
                        if (column.equalsIgnoreCase("tripStartTime"))
                            time = i;
                        if (column.equalsIgnoreCase("truckType"))
                            truckType = i;
                        if (column.equalsIgnoreCase("truckVolume"))
                            trucks = i;
                        if (column.equalsIgnoreCase("travelTime"))
                        	duration = i;
                    }
                    continue;
                }
                int o = zonesIndexMap.get(Integer.parseInt(data[origin]));
                int d = zonesIndexMap.get(Integer.parseInt(data[dest]));
                float traveltime = ctSkims[o][d];
                TimePeriod t = timePeriodMapping[newStartTime(data[time], (int) traveltime)];
                double trip = (trucks > -1) ? Double.parseDouble(data[trucks]) : 1.0f;
                trips.get(AssignMode.TRUCK).get(t)[o][d] += trip;
            }
        }  catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void writeDemandMatrices(Map<AssignMode,Map<TimePeriod,double[][]>> trips, Map<Integer,Integer> zoneIndex) {
        int[] externalNumbers = new int[zoneIndex.size()+1];
        for (int zone : zoneIndex.keySet())
            externalNumbers[zoneIndex.get(zone)+1] = zone;
        String tokenizedDemandFile = resourceBundle.getString(DEMAND_OUTPUT_FILE_NAME_PROPERTY);
        for (AssignMode mode : AssignMode.values()) {
            for (TimePeriod period : TimePeriod.values()) {
                File demandFile = new File(tokenizedDemandFile.replace(DEMAND_OUTPUT_MODE_STRING,mode.getModeId())
                                                              .replace(DEMAND_OUTPUT_TIME_PERIOD_STRING,period.getPeriodId()));
               String mName = demandFile.getName();
               mName = mName.substring(0,mName.indexOf("."));
               Matrix m = new Matrix(mName,mName,doubleToFloatArray(trips.get(mode).get(period)));
               m.setExternalNumbers(externalNumbers);
               ZipMatrixWriter zmw = new ZipMatrixWriter(demandFile);
               zmw.writeMatrix(m);
            }
        }
    }

    private float[][] doubleToFloatArray(double[][] doubleArray) {
        float[][] floatArray = new float[doubleArray.length][doubleArray.length];
        for (int i = 0; i < floatArray.length; i++)
            for (int j = 0; j < floatArray.length; j++)
                floatArray[i][j] = (float) doubleArray[i][j];
        return floatArray;
    }

    private static enum TimePeriod {
        AM("am","peak"),
        PM("pm","peak"),
        MD("md","offpeak"),
        NT("nt","offpeak");

        private final String idPrefix;
        private final String idSuffix;

        private TimePeriod(String idPrefix, String idSuffix) {
            this.idPrefix = idPrefix;
            this.idSuffix = idSuffix;
        }

        public String getPeriodId() {
            return idPrefix+idSuffix;
        }
    }

    private static enum AssignMode {
        AUTO("a"),
        INTERCITY("intercity"),
        INTRACITY("intracity"),
        TRUCK("d");

        private final String modeId;

        private AssignMode(String modeId) {
            this.modeId = modeId;
        }

        public String getModeId() {
            return modeId;
        }

        public static AssignMode getAssignModeForId(String id) {
            for (AssignMode mode : AssignMode.values())
                if (mode.modeId.equals(id))
                    return mode;
            throw new IllegalArgumentException("Unknown AssignMode id: " + id);
        }
    }

    private static enum Mode {
        DA(AssignMode.AUTO),
        SR2(AssignMode.AUTO),
        SR3P(AssignMode.AUTO),
        INTERCITY(AssignMode.INTERCITY),
        INTRACITY(AssignMode.INTRACITY),
        TRUCK(AssignMode.TRUCK);

        private final AssignMode assignMode;

        private Mode(AssignMode assignMode) {
            this.assignMode = assignMode;
        }

        public static Mode getModeForModeString(String modeString) {
            if (modeString.equalsIgnoreCase(TripModeType.DA.name()))
                return DA;
            if (modeString.equalsIgnoreCase(TripModeType.SR2.name()))
                return SR2;
            if (modeString.equalsIgnoreCase(TripModeType.SR3P.name()))
                return SR3P;
            if (modeString.equalsIgnoreCase(LDTripModeType.TRANSIT_WALK.name()))
                return INTERCITY;
            if (modeString.equalsIgnoreCase(TripModeType.WK_TRAN.name()))
                return INTRACITY;
            return null;
        }

        public AssignMode getAssignMode() {
            return assignMode;
        }
    }

    public static void main(String ... args) {
        ResourceBundle properties = ResourceUtil.getPropertyBundle(new File(args[0]));
        new DemandBuilder(properties).writeDemandMatrices();
    }
}
