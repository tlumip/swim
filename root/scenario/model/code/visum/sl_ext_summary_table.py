# This script creates EI/EE/IE tables using SWIM trip data
# It can also create EI/EE/IE tables by time period (AM, MD)

import os, shutil, sys, csv, time, struct, zipfile
import pandas as pd
import numpy as np
sys.path.append(os.path.join(os.getcwd(),'model','code'))
sys.path.append(os.path.join(os.getcwd(),'model','code', 'visum'))
from Properties import Properties

########################
# Read properties file #
#load properties
if len(sys.argv) < 2:
    print(len(sys.argv))
    print(sys.argv)
    print("missing arguments!")
    sys.exit(1)

property_file = sys.argv[1]
properties = Properties()
properties.loadPropertyFile(property_file)

##############
# Load trips #
# ET -> Empty file but still created as part of SWIM extraction process (ignore)
# CT -> Commercial ldtvicles (Trucks)
# LDTPerson -> Lond Distance Trip Person
# SDTPerson -> Short Distance Trip Person
# LDTVehicle -> Long Distance Trip ldtvicle (same trips as LDTPerson). ODOT replaced LDTV for LDTP.

# Function to load data
def load_trip_data(dict_file_path):
    """Load trip data files from a specified directory."""
    return {name: pd.read_csv(file) for name, file in dict_file_path.items()}

def select_link_filenames(par_dir, file_path):
    base_name, ext = os.path.splitext(os.path.basename(file_path))
    new_file_path = os.path.join(par_dir, base_name + "_select_link" + ext)
    return new_file_path

def unzip_output(out_zip_file):
    basename = os.path.basename(out_zip_file)
    output_folder = os.path.dirname(out_zip_file)
    out_directory = basename.split('.')[0]
    extracted_directory = os.path.join(output_folder, out_directory)

    if not os.path.exists(extracted_directory):
        os.makedirs(extracted_directory)


    #unzip files
    zip_ref = zipfile.ZipFile(out_zip_file,'r')
    zip_ref.extractall(extracted_directory)
    zip_ref.close()

    return(extracted_directory)

########################
# Function Definitions #
def convert_zone_to_int(zone):
    """Converts origin/dest zone format from string (e.g. "_7") to int."""
    if "_" in zone:
        return int(zone.split("_")[1])
    else:
        return int(zone)

def add_time_period(time, time_periods):
    """Classifies trips based on their start time"""
    for period, (start, end) in time_periods.items():
        if start <= end:
            if start <= time <= end:
                return period
        else:  # Handles overnight wrap (e.g., 1800 to 659)
            if time >= start or time <= end:
                return period
    return "OFF"


def format_tables(ldtv, sdtp, ct, mode_map, per_to_veh, time_periods, swim_to_lcog_xwalk=None, ext_zones=np.arange(1,24)):
    cols = ['tripMode', 'EXTERNAL_ZONE_ORIGIN', 'EXTERNAL_ZONE_DESTINATION', 'tripStartTime', 'origin', 'destination']

    auto = pd.concat([ldtv[cols], sdtp[cols]], axis=0)
    
    auto = auto[auto['tripMode'].isin(per_to_veh.keys())].copy()
    trips = {'auto' : auto, 'truck': ct[cols]}
    
    # Format orign/dest column and determine if EE/EI/IE  
    for df in trips.keys():
        # Fill missing values
        trips[df]['EXTERNAL_ZONE_ORIGIN'] = trips[df]['EXTERNAL_ZONE_ORIGIN'].fillna('-1')
        trips[df]['EXTERNAL_ZONE_DESTINATION'] = trips[df]['EXTERNAL_ZONE_DESTINATION'].fillna('-1')
        origin_filter = trips[df]['EXTERNAL_ZONE_ORIGIN']=='null'
        trips[df].loc[origin_filter,'EXTERNAL_ZONE_ORIGIN'] = trips[df].loc[origin_filter,'origin'].astype(str)
        dest_filter = trips[df]['EXTERNAL_ZONE_DESTINATION']=='null'
        trips[df].loc[dest_filter,'EXTERNAL_ZONE_DESTINATION'] = trips[df].loc[dest_filter,'destination'].astype(str)
        
        # Convert zone to int and define time period
        trips[df]['ext_origin'] = trips[df]['EXTERNAL_ZONE_ORIGIN'].apply(convert_zone_to_int)
        trips[df]['ext_dest'] = trips[df]['EXTERNAL_ZONE_DESTINATION'].apply(convert_zone_to_int)
        trips[df]['time_period'] = trips[df]['tripStartTime'].apply(lambda t: add_time_period(t, time_periods))
        
        trips[df].loc[trips[df]['ext_origin'].isin(ext_zones) & trips[df]['ext_dest'].isin(ext_zones), 'trip_type'] = 'EE'
        trips[df].loc[trips[df]['ext_origin'].isin(ext_zones) & ~trips[df]['ext_dest'].isin(ext_zones), 'trip_type'] = 'EI'
        trips[df].loc[~trips[df]['ext_origin'].isin(ext_zones) & trips[df]['ext_dest'].isin(ext_zones), 'trip_type'] = 'IE'
        
        # Convert zones to LCOG zone system
        if swim_to_lcog_xwalk is not None:
            trips[df].loc[~trips[df]['ext_origin'].isin(ext_zones), 'ext_origin'] = (
                trips[df].loc[~trips[df]['ext_origin'].isin(ext_zones), 'ext_origin'].map(swim_to_lcog_xwalk))
            
            trips[df].loc[~trips[df]['ext_dest'].isin(ext_zones), 'ext_dest'] = (
                trips[df].loc[~trips[df]['ext_dest'].isin(ext_zones), 'ext_dest'].map(swim_to_lcog_xwalk))

    # Convert person trips to vehicle trips
    trips['truck']['veh_trip'] = 1
    trips['truck']['mode'] = 'truck'
    trips['auto']['veh_trip'] = trips['auto']['tripMode'].map(per_to_veh)
    trips['auto']['mode'] = trips['auto']['tripMode'].map(mode_map)
    
    return trips

def create_ext_table_by_time(df, periods, modes, year, ext_zones=np.arange(1,24)):
    """Creates EE/EI/IE tables by time period by mode"""
    table = {}
    for period in periods:
        table[period] = pd.DataFrame(index = ext_zones)
        for tt in ['EI', 'IE', 'EE']:
            for mode in modes:
                if tt == 'IE':
                    gb_col = 'ext_dest'
                else:
                    gb_col = 'ext_origin'
                
                temp = (df[(df['time_period'] == period) & (df['mode'] == mode) & (df['trip_type'] == tt)]
                    .groupby(gb_col)['veh_trip'].sum()
                    ).fillna(0)
            
                if tt == 'EE':
                    temp2 = (df[(df['time_period'] == period) & (df['mode'] == mode) & (df['trip_type'] == tt)]
                    .groupby('ext_dest')['veh_trip'].sum()
                    ).fillna(0)
                    temp = temp.add(temp2, fill_value = 0)
                
                table[period]["{}_{}_{}".format(tt, mode, year)] = temp.round(2)
        table[period] = table[period].fillna(0)
    return table

def create_int_ext_table(df, modes, year, ext_zones=np.arange(1,24)):
    table = pd.DataFrame(index = ext_zones)
    for tt in ['EI', 'IE', 'EE']:
        for mode in modes:
            if tt == 'IE':
                gb_col = 'ext_dest'
            else:
                gb_col = 'ext_origin'
                
            temp = (df[(df['mode'] == mode) & (df['trip_type'] == tt)]
                    .groupby(gb_col)['veh_trip'].sum()
                    ).fillna(0)
            
            if tt == 'EE':
                temp2 = (df[(df['mode'] == mode) & (df['trip_type'] == tt)]
                .groupby('ext_dest')['veh_trip'].sum()
                ).fillna(0)
                temp = temp.add(temp2, fill_value = 0)
            table["{}_{}_{}".format(tt, mode, year)] = temp.round(2)
    table = table.fillna(0)
    return table

def main():
    #zip outputs
    print('Unzip select link outputs')
    data_dir = unzip_output(properties['sl.output.bundle.file'])

    print("Generating external trip summaries")
    # Load extenalstaions
    slink = pd.read_csv(properties['sl.input.file.select.links'])

    trip_files = {
        'ct': properties['ct.truck.trips'],
        'et': properties['et.truck.trips'],
        'ldtp': properties['ldt.person.trips'],
        'sdtp': properties['sdt.person.trips'],
        'ldtv': properties['ldt.vehicle.trips']
    }
    
    trip_files = {k:select_link_filenames(data_dir, v) for k,v in trip_files.items()}
    # Load SWIM trip data
    trips = load_trip_data(trip_files)

    #################
    # Create tables #
    
    # Define maps
    per_to_veh = {'DA': 1, 'SR2': 0.5,'SR3P': 1/3.5} # Person trips to veh trips conversion rates 
    mode_map = {'DA': 'SOV', 'SR2': 'HOV', 'SR3P': 'HOV'} 
    modes = ['SOV', 'HOV', 'truck']
    # ext_zones = np.arange(1, properties.parseInt('ext.numextzones')+1) # 23 ext zones
    ext_zones = slink.STATIONNUMBER.sort_values().unique() # 23 ext zones
    
    # Define time periods for IE/EI/EE tables by time-period
    ##start time, end time of time period
    time_periods = {'AM': (properties.parseInt('am.peak.start'), properties.parseInt('am.peak.end')), 
                    'MD': (properties.parseInt('md.offpeak.start'), properties.parseInt('md.offpeak.end')),
                    'PM': (properties.parseInt('pm.peak.start'), properties.parseInt('pm.peak.end')),
                    'NT': (properties.parseInt('nt.offpeak.start'), properties.parseInt('nt.offpeak.end'))}
    periods = ['AM', 'MD', 'PM', 'NT']
    
    # Format tables 
    trips_clean = format_tables(trips['ldtp'], trips['sdtp'], trips['ct'],
                                   mode_map, per_to_veh, time_periods, ext_zones=ext_zones)
    
    # Create EE/IE/EI tables
    df_merged = pd.concat([trips_clean['auto'], trips_clean['truck']], axis=0)
    ext_table = create_int_ext_table(df_merged, modes, ext_zones=ext_zones, year=properties.parseInt('t.year'))
    
    # Merge and save
    out_dir = os.path.dirname(properties['sl.output.bundle.file'])
    ext_table.rename_axis('STATIONNUMBER').to_csv(os.path.join(out_dir, properties['sl.output.file.select.link.external_table']))
    
    #########################
    # Tables by time period #
    
    # Create EE/IE/EI tables by time period for 2025
    ext_table_by_time = create_ext_table_by_time(df_merged, 
                                                 periods, 
                                                 modes, 
                                                 ext_zones=ext_zones, 
                                                 year=properties.parseInt('t.year'))
    
    # Save
    for k in ext_table_by_time.keys():
        outfile = os.path.splitext(properties['sl.output.file.select.link.external_table'])[0] + '_' + k + '.csv'
        ext_table_by_time[k].rename_axis('STATIONNUMBER').to_csv(os.path.join(out_dir, outfile))
    #remove the un-zipped folder
    print('Delete unzipped outputs')
    if os.path.isdir(data_dir):
        shutil.rmtree(data_dir)


if __name__ == "__main__":
    main()