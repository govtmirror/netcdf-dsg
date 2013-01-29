package gov.usgs.cida.netcdf.dsg;

import java.nio.ByteBuffer;
import java.io.Closeable;
import com.sun.jna.NativeLong;
import com.sun.jna.ptr.IntByReference;
import java.io.File;
import static gov.usgs.cida.netcdf.jna.NC.*;
import static gov.usgs.cida.netcdf.jna.NCUtil.*;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 *
 * @author Tom Kunicki <tkunicki@usgs.gov>
 */
public class StationTimeSeriesMultiDimensional implements Closeable {
    
    public static final String CF_VER = "CF-1.6";

    private final static String STATION = "station";
    private final static String STATION_ID_LEN = "station_id_len";
    protected final static String TIME = "time";
    protected final static String RECORD_STRUCT = "record"; // NetCDF-Java reqiures this to be record (last tested release was 4.2.26)
    
    // These parameters are supported by the CF standared but NetCDF-Java
    // does't support alternatives (last tested 4.3.15)
    private final boolean stationOuter = true;  
    private final boolean useStructure = false;  // NetCDF-Java can't handle 'true' (last tested 4.3.15)
    
    // unlimited on outer dimension allows data to be appended to existing file
    // but results in some spares reads...
    private final boolean useUnlimited = false;   
    
    public final int ncId;
    
    public final int ncDimId_station;
    public final int ncDimId_station_id_len;
    public final int ncDimId_time;
    
    private Variable[] recordVariables;
    private int record_type_size;
    
    private int ncTypeId_record_type;
    private int ncVarId_record;
    
    private int[] ncVarId_records;
    
    private boolean isClosed = false;
    
    

    /**
     * Put this in place to keep the interface the same for anything using this currently
     * @param file Output NetCDF file
     * @param recordType RecordType matching the 
     * @param chunking
     * @param stations 
     */
    public StationTimeSeriesMultiDimensional(
            File file,
            Map<String,String> globalAttrs,
            Station[] stations,
            int[] timeOffsets,
            String timeUnit,
            Variable[] recordVariables)
    {
        this.recordVariables = recordVariables;
        
        IntByReference iRef = new IntByReference();

        status(nc_create(file.getAbsolutePath(), NC_NETCDF4, iRef));
        ncId = iRef.getValue();

        //// DIMENSIONS:
        status(nc_def_dim(
                ncId, STATION,
                new NativeLong(stationOuter && useUnlimited ? NC_UNLIMITED : stations.length),
                iRef));
        ncDimId_station = iRef.getValue();

        int max_length = Station.findMaxStationLength(stations);
        status(nc_def_dim(ncId, STATION_ID_LEN, new NativeLong(max_length), iRef));
        ncDimId_station_id_len = iRef.getValue();

        status(nc_def_dim(
                ncId, TIME,
                new NativeLong(!stationOuter && useUnlimited ? NC_UNLIMITED : timeOffsets.length),
                iRef));
        ncDimId_time = iRef.getValue();
        
        //// DOMAIN VARIABLES
        int[] station_dimidsp = new int[] { ncDimId_station };
        int[] station_id_dimidsp = new int[] { ncDimId_station, ncDimId_station_id_len };
        int[] time_dimidsp = new int[] { ncDimId_time };

        status(nc_def_var(ncId, "station_id", NC_CHAR, station_id_dimidsp , iRef));
        int ncVarId_station_id = iRef.getValue();
        status(nc_put_att_text(ncId, ncVarId_station_id, "standard_name", "station_id"));
        status(nc_put_att_text(ncId, ncVarId_station_id, "cf_role", "timeseries_id"));
        
        status(nc_def_var(ncId, "lat", NC_FLOAT, station_dimidsp, iRef));
        int ncVarId_lat = iRef.getValue();
        status(nc_put_att_text(ncId, ncVarId_lat, "standard_name", "latitude"));
        status(nc_put_att_text(ncId, ncVarId_lat, "units", "degrees_east"));
        
        status(nc_def_var(ncId, "lon", NC_FLOAT, station_dimidsp, iRef));
        int ncVarId_lon = iRef.getValue();
        status(nc_put_att_text(ncId, ncVarId_lon, "standard_name", "longitude"));
        status(nc_put_att_text(ncId, ncVarId_lon, "units", "degrees_north"));
        
        status(nc_def_var(ncId, "time", NC_INT, time_dimidsp, iRef));
        int ncVarId_time = iRef.getValue();
        status(nc_put_att_text(ncId, ncVarId_time, "standard_name", "time"));
        status(nc_put_att_text(ncId, ncVarId_time, "units", timeUnit));
        
        // Global Attributes
        writeGlobalAttributes(globalAttrs);

        if (useStructure) {
            createRecordStructureType();
            createRecordStructureVariable();
        } else {
            createRecordVariables();
        }

        status(nc_enddef(ncId));
        
        // Now lets fill in the Station data from vararg stations
        // Then we're set up to start putting observations
        NativeLong station_indexp = new NativeLong(0);
        NativeLong stationid_len_indexp = new NativeLong(0);
        for (Station station : stations) {
            status(nc_put_vara_text(ncId, ncVarId_station_id, station.station_id, station_indexp, stationid_len_indexp));
            status(nc_put_var1_float(ncId, ncVarId_lon, station.longitude, station_indexp));
            status(nc_put_var1_float(ncId, ncVarId_lat, station.latitude, station_indexp));
            station_indexp.setValue(station_indexp.longValue() + 1);
        }
        
        NativeLong time_indexp = new NativeLong(0);
        for (int t = 0; t < timeOffsets.length; ++t) {
            status(nc_put_var1_int(ncId, ncVarId_time, timeOffsets[t], time_indexp));
            time_indexp.setValue(time_indexp.longValue() + 1);
        }
    }
    
    private void writeGlobalAttributes(Map<String, String> attrMap) {
        status(nc_put_att_text(ncId, NC_GLOBAL, "Conventions", CF_VER));
        status(nc_put_att_text(ncId, NC_GLOBAL, "CF:featureType", "timeSeries"));
        if (null != attrMap) {
            for(String key : attrMap.keySet()) {
                status(nc_put_att_text(ncId, NC_GLOBAL, key, attrMap.get(key)));
            }
        }
    }
    
    private void createRecordStructureType() {
        IntByReference iRef = new IntByReference();
        
        record_type_size = 0;
        for (Variable recordVariable : recordVariables) {
            record_type_size += recordVariable.xtype.getSizeBytes();
        }

        // Create record_type Compound
        status(nc_def_compound(ncId, new NativeLong(record_type_size), RECORD_STRUCT + "_type", iRef));
        ncTypeId_record_type = iRef.getValue();

        // Populate record_type Compound
        int offset = 0;
        for (Variable recordVariable : recordVariables) {
            XType type = recordVariable.xtype;
            status(nc_insert_compound(ncId, ncTypeId_record_type, recordVariable.name, new NativeLong(offset), type.getCode()));
            offset += type.getSizeBytes();  // offset at the end should be the same as compound_size
        }
    }

    private void createRecordStructureVariable() {
        
        IntByReference iRef = new IntByReference();
        int[] record_dimidsp = stationOuter ? 
                new int[] { ncDimId_station, ncDimId_time } :
                new int[] { ncDimId_time, ncDimId_station };
        
        String structName = RECORD_STRUCT;

        status(nc_def_var(ncId, structName, ncTypeId_record_type, record_dimidsp, iRef));
        ncVarId_record = iRef.getValue();


        int recordVariableCount = recordVariables.length;
        List<String> recordVariableNameList = new ArrayList<String>(recordVariableCount);
        Map<String, List[]> attributeMap = new LinkedHashMap<String, List[]>();
        for (Variable recordVariable : recordVariables) {
            String variableName = recordVariable.name;
            recordVariableNameList.add(variableName);
            for (Map.Entry<String,Object> entry : recordVariable.attributes.entrySet()) {
                String attributeName = entry.getKey();
                List[] values = attributeMap.get(entry.getKey());
                if (null == values) {
                    values = (LinkedList[]) new LinkedList[2];
                    values[0] = new LinkedList();
                    values[1] = new LinkedList();
                    attributeMap.put(attributeName, values);
                }
                values[0].add(variableName);
                values[1].add(entry.getValue());
            }
        }
       
        //status(nc_put_att_text(ncId, ncVarId_record, "coordinates", "time lat lon"));
        String[] coordinateAttributeValues = new String[recordVariableCount];
        Arrays.fill(coordinateAttributeValues, "time lat lon");
        generateCompoundAttributes(ncId, ncVarId_record,
                structName + "_coordinates_type",
                "coordinates",
                recordVariableNameList.toArray(new String[0]),
                coordinateAttributeValues);

        for (String attr : attributeMap.keySet()) {
            String compoundName = structName + "_" + attr + "_type";
            List<String>[] tuples = attributeMap.get(attr);
            String[] varNames = new String[tuples[0].size()];
            Object[] values = new Object[tuples[1].size()];
            tuples[0].toArray(varNames);
            tuples[1].toArray(values);
            if (values[0] instanceof String) {
                String[] strVals = new String[values.length];
                for (int i=0; i<values.length; i++) {
                    strVals[i] = (String)values[i];
                }
                generateCompoundAttributes(ncId, ncVarId_record, compoundName,
                                           attr, varNames, strVals);
            }
            else if (values[0] instanceof Integer) {
                int[] intVals = new int[values.length];
                for (int i = 0; i < values.length; i++) {
                    intVals[i] = ((Integer) values[i]).intValue();
                }
                generateCompoundAttributes(ncId, ncVarId_record, compoundName,
                                           attr, varNames, intVals);
            }
            else if (values[0] instanceof Short) {
                short[] shortVals = new short[values.length];
                for (int i = 0; i < values.length; i++) {
                    shortVals[i] = ((Short) values[i]).shortValue();
                }
                generateCompoundAttributes(ncId, ncVarId_record, compoundName,
                                           attr, varNames, shortVals);
            }
            else if (values[0] instanceof Float) {
                float[] floatVals = new float[values.length];
                for (int i = 0; i < values.length; i++) {
                    floatVals[i] = ((Float) values[i]).floatValue();
                }
                generateCompoundAttributes(ncId, ncVarId_record, compoundName,
                                           attr, varNames, floatVals);
            }
            else if (values[0] instanceof Double) {
                double[] doubleVals = new double[values.length];
                for (int i = 0; i < values.length; i++) {
                    doubleVals[i] = ((Double) values[i]).doubleValue();
                }
                generateCompoundAttributes(ncId, ncVarId_record, compoundName,
                                           attr, varNames, doubleVals);
            }
            else {
                throw new UnsupportedOperationException("Should implement the other types");
            }
        }
    }
    
    private void createRecordVariables() {
        
        IntByReference iRef = new IntByReference();
        int[] record_dimidsp = stationOuter ? 
                new int[] { ncDimId_station, ncDimId_time } :
                new int[] { ncDimId_time, ncDimId_station };
       
        int vCount = recordVariables.length;
        ncVarId_records = new int[vCount];
        for (int vIndex = 0; vIndex < vCount; ++vIndex) {
        
            Variable recordVariable = recordVariables[vIndex];
            
            status(nc_def_var(ncId, recordVariable.name, recordVariable.xtype.getCode(), record_dimidsp, iRef));
            ncVarId_records[vIndex] = iRef.getValue();

            // Can I pull this out in a generalized way
            status(nc_put_att_text(ncId,  ncVarId_records[vIndex], "coordinates", "time lat lon"));

            for (Map.Entry<String, Object> entry : recordVariable.attributes.entrySet()) {
                String name = entry.getKey();
                Object value = entry.getValue();
                if (value instanceof String) {
                    status(nc_put_att_text(ncId, ncVarId_records[vIndex], name, (String)value));
                }
                else if (value instanceof Integer) {
                    status(nc_put_att_int(ncId, ncVarId_records[vIndex], name, (Integer)value));
                }
                else if (value instanceof Short) {
                    status(nc_put_att_short(ncId, ncVarId_records[vIndex], name, (Short)value));
                }
                else if (value instanceof Float) {
                    status(nc_put_att_float(ncId, ncVarId_records[vIndex], name, (Float)value));
                }
                else if (value instanceof Double) {
                    status(nc_put_att_double(ncId, ncVarId_records[vIndex], name, (Double)value));
                }
                else {
                    throw new UnsupportedOperationException("Should implement the other types");
                }
            }
        }
    }
    
    public boolean putObservation(Observation observation) {        
        return useStructure ?
                putObservationIntoStructureVariable(observation) :
                putObservationIntoVariables(observation);
    }
    
    public boolean putObservationIntoStructureVariable(Observation observation) {
        
        if (!isClosed) {
            ByteBuffer recordBuffer = ByteBuffer.allocateDirect(record_type_size);
            recordBuffer.order(ByteOrder.nativeOrder());
            
            for (Object value : observation.values) {
                if (value instanceof String) {
                    throw new UnsupportedOperationException("Need to implement char[] or string");
                }
                else if (value instanceof Integer) {
                    recordBuffer.putInt(((Integer)value));
                }
                else if (value instanceof Short) {
                    recordBuffer.putShort((Short) value);
                }
                else if (value instanceof Float) {
                    recordBuffer.putFloat((Float) value);
                }
                else if (value instanceof Double) {
                    recordBuffer.putDouble((Double) value);
                }
                else {
                    throw new UnsupportedOperationException("Should implement the other types");
                }
            } // record complete
            
            recordBuffer.rewind();
            NativeLong[] record_startp = stationOuter ? 
                    new NativeLong[] { 
                        new NativeLong(observation.station_index),
                        new NativeLong(observation.time_offset),
                    } :
                    new NativeLong[] { 
                        new NativeLong(observation.time_offset),
                        new NativeLong(observation.station_index),
                    };
            NativeLong[] record_countp = new NativeLong[] {
                new NativeLong(1),
                new NativeLong(1),
            };
            status(nc_put_vara(ncId, ncVarId_record, record_startp, record_countp, recordBuffer));
                        
            return true;
        }
        return false;
    }
    
    public boolean putObservationIntoVariables(Observation observation) {
        
        if (!isClosed) {
            
            NativeLong[] record_indexp = stationOuter ? 
                new NativeLong[] { 
                    new NativeLong(observation.station_index),
                    new NativeLong(observation.time_offset),
                } :
                new NativeLong[] { 
                    new NativeLong(observation.time_offset),
                    new NativeLong(observation.station_index),
                };
            
            int vCount = ncVarId_records.length;
            for (int vIndex = 0; vIndex < vCount; ++vIndex) {
                Object value = observation.values[vIndex];
                if (value instanceof String) {
                    throw new UnsupportedOperationException("Need to implement char[] or string");
                }
                else if (value instanceof Integer) {
                    status(nc_put_var1_int(ncId, ncVarId_records[vIndex], (Integer)value, record_indexp));
                }
                else if (value instanceof Short) {
                    status(nc_put_var1_short(ncId, ncVarId_records[vIndex], (Short)value, record_indexp));
                }
                else if (value instanceof Float) {
                    status(nc_put_var1_float(ncId, ncVarId_records[vIndex], (Float)value, record_indexp));
                }
                else if (value instanceof Double) {
                    status(nc_put_var1_double(ncId, ncVarId_records[vIndex], (Double)value, record_indexp));
                }
                else {
                    throw new UnsupportedOperationException("Should implement the other types");
                }
            }
            return true;
        }
        return false;
    }

    @Override
    public void close() {
        status(nc_close(ncId));
        isClosed = true;
    }

    public void sync() {
        status(nc_sync(ncId));
    }
}

