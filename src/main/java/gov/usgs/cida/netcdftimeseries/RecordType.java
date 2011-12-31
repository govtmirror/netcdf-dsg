package gov.usgs.cida.netcdftimeseries;

import java.util.HashMap;
import java.util.Map;
import java.util.LinkedHashMap;
import com.sun.jna.NativeLong;
import com.sun.jna.ptr.IntByReference;
import java.util.LinkedList;
import java.util.List;
import static gov.usgs.cida.netcdf.jna.NC.*;
import static gov.usgs.cida.netcdf.jna.NCUtil.*;

/**
 *
 * @author Jordan Walker <jiwalker@usgs.gov>
 */
public class RecordType {
    private static final int NON_STATISTIC_VARIABLE_COUNT = 4;

    // Complicated structure, attribute name -> 2 lists (variables{string}, values{Object})
    private LinkedHashMap<String, List[]> attributeMap = new LinkedHashMap<String, List[]>();
    private List<Variable> typeList;
    private int compound_size;
    private int record_var_id;

    public RecordType(String timeUnits) {
        typeList = new LinkedList<Variable>();
        compound_size = 0;

        // These have to be the first two in the record type, everything else is statistics
        addType(Variable.createTimeVariable(timeUnits));
        addType(Variable.createStationIdVariable());
        addType(Variable.createLatitude());
        addType(Variable.createLongitude());
    }

    public final boolean addType(Variable var) {
        if (var.vtype != Variable.VariableType.LAT_LON) {
            compound_size += var.xtype.getSizeBytes();

            // Create a combined map of attributes to those variables that have them
            if (var.vtype != Variable.VariableType.STATION_ID) {
                for (String key : var.attributes.keySet()) {
                    Object value = var.attributes.get(key);
                    addToAttributeMap(var.name, key, value);
                }
            }
        }
        return typeList.add(var);
    }

    public int writeRecordCompound(int ncId) {
        int ncStatus;
        IntByReference iRef = new IntByReference();

        // Create record_type Compound
        ncStatus = nc_def_compound(ncId, new NativeLong(compound_size),
                                   StationTimeSeriesNetCDFFile.OBSERVATION_STRUCT_NAME + "_type",
                                   iRef);
        status(ncStatus);
        int ncTypeId_record_type = iRef.getValue();

        // Populate record_type Compound
        int offset = 0;
        for (Variable var : typeList) {
            if (var.vtype != Variable.VariableType.LAT_LON) {
                XType type = var.xtype;
                String attrName = (var.vtype == Variable.VariableType.STATION_ID) ? "index" : var.name;
                ncStatus = nc_insert_compound(ncId, ncTypeId_record_type,
                                              attrName, new NativeLong(offset),
                                              type.getCode());
                status(ncStatus);
                offset += type.getSizeBytes();  // offset at the end should be the same as compound_size
            }
        }

        return ncTypeId_record_type;
    }

    public Map<String,Variable> writeStationVariables(int ncId, int ncDimId_station, int ncDimId_station_id_len) {
        int[] station_dimidsp;
        int[] station_id_dimidsp;
        int ncStatus;
        IntByReference iRef = new IntByReference();
        Map<String,Variable> returnMap = new HashMap<String,Variable>();
            
        station_dimidsp = new int[] { ncDimId_station };
        station_id_dimidsp = new int[] { ncDimId_station, ncDimId_station_id_len };

        for (Variable var : typeList) {
            if (var.vtype == Variable.VariableType.LAT_LON
                || var.vtype == Variable.VariableType.STATION_ID) {
                returnMap.put(var.name, var);
                
                if (var.vtype == Variable.VariableType.STATION_ID) {
                    ncStatus = nc_def_var(ncId, "station_id", NC_CHAR, 
                                      station_id_dimidsp , iRef);
                }
                else {
                    ncStatus = nc_def_var(ncId, var.name, var.xtype.getCode(),
                                      station_dimidsp, iRef);
                }
                status(ncStatus);
                int ncVarId = iRef.getValue();
                var.ncVarId = ncVarId;
                for (String key : var.attributes.keySet()) {
                    Object attrValue = var.attributes.get(key);

                    if (attrValue instanceof String) {
                        String val = (String) attrValue;
                        ncStatus = nc_put_att_text(ncId, ncVarId, key, val);
                        status(ncStatus);
                    }
                    else if (attrValue instanceof Integer) {
                        int val = ((Integer) attrValue).intValue();
                        ncStatus = nc_put_att_int(ncId, ncVarId, key, val);
                        status(ncStatus);
                    }
                    else if (attrValue instanceof Short) {
                        short val = ((Short) attrValue).shortValue();
                        ncStatus = nc_put_att_short(ncId, ncVarId, key, val);
                        status(ncStatus);
                    }
                    else if (attrValue instanceof Float) {
                        float val = ((Float) attrValue).floatValue();
                        ncStatus = nc_put_att_float(ncId, ncVarId, key, val);
                        status(ncStatus);
                    }
                    else if (attrValue instanceof Double) {
                        double val = ((Double) attrValue).doubleValue();
                        ncStatus = nc_put_att_double(ncId, ncVarId, key, val);
                        status(ncStatus);
                    }
                    else {
                        throw new UnsupportedOperationException(
                                "Should implement the other types");
                    }
                }
            }
        }

        return returnMap;
    }

    public void writeObservationVariables(int ncId, int ncDimId_observation,
                                             int ncTypeId_record_type, boolean doChunking) {
        int ncStatus;
        IntByReference iRef = new IntByReference();
        int[] record_dimidsp = new int[] { ncDimId_observation };
        String structName = StationTimeSeriesNetCDFFile.OBSERVATION_STRUCT_NAME;

        ncStatus = nc_def_var(ncId, structName, ncTypeId_record_type,
                              record_dimidsp, iRef);
        status(ncStatus);
        record_var_id = iRef.getValue();
        

        // Can I pull this out in a generalized way
        ncStatus = nc_put_att_text(ncId, record_var_id, "coordinates",
                                   structName + ".time lon lat");

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
                generateCompoundAttributes(ncId, record_var_id, compoundName,
                                           attr, varNames, strVals);
            }
            else if (values[0] instanceof Integer) {
                int[] intVals = new int[values.length];
                for (int i = 0; i < values.length; i++) {
                    intVals[i] = ((Integer) values[i]).intValue();
                }
                generateCompoundAttributes(ncId, record_var_id, compoundName,
                                           attr, varNames, intVals);
            }
            else if (values[0] instanceof Short) {
                short[] shortVals = new short[values.length];
                for (int i = 0; i < values.length; i++) {
                    shortVals[i] = ((Short) values[i]).shortValue();
                }
                generateCompoundAttributes(ncId, record_var_id, compoundName,
                                           attr, varNames, shortVals);
            }
            else if (values[0] instanceof Float) {
                float[] floatVals = new float[values.length];
                for (int i = 0; i < values.length; i++) {
                    floatVals[i] = ((Float) values[i]).floatValue();
                }
                generateCompoundAttributes(ncId, record_var_id, compoundName,
                                           attr, varNames, floatVals);
            }
            else if (values[0] instanceof Double) {
                double[] doubleVals = new double[values.length];
                for (int i = 0; i < values.length; i++) {
                    doubleVals[i] = ((Double) values[i]).doubleValue();
                }
                generateCompoundAttributes(ncId, record_var_id, compoundName,
                                           attr, varNames, doubleVals);
            }
            else {
                throw new UnsupportedOperationException(
                        "Should implement the other types");
            }
        }
                
        // Now do ragged_parent_index
        String[] fields = {"index"};
        String[] values = {"station"};
        generateCompoundAttributes(ncId, record_var_id, structName + "_ragged_parent_index_type",
                                           "CF:ragged_parent_index", fields, values);
        
        if (doChunking) {
            ncStatus = nc_def_var_chunking(ncId, record_var_id, NC_CHUNKED, new NativeLong[] { new NativeLong(compound_size) });
            status(ncStatus);
        }
    }
    
    public void writeGlobalAttributes(int ncId, LinkedHashMap<String, String> attrMap) {
        int ncStatus;
        ncStatus = nc_put_att_text(ncId, NC_GLOBAL, "Conventions", "CF-1.6"); status(ncStatus);
        ncStatus = nc_put_att_text(ncId, NC_GLOBAL, "CF:featureType", "timeSeries"); status(ncStatus);
        if (null != attrMap) {
            for(String key : attrMap.keySet()) {
                ncStatus = nc_put_att_text(ncId, NC_GLOBAL, key, attrMap.get(key)); status(ncStatus);
            }
        }
    }
    
    public int getCompoundSize() {
        return compound_size;
    }
    
    public int getVarId() {
        return record_var_id;
    }
    
    public boolean isObservationValid(Observation observation) {
        int statisticCount = 0;
        for (Variable type : typeList) {
            if (type.vtype == Variable.VariableType.STATISTIC) {
                
                if (statisticCount < observation.values.length &&
                    observation.values[statisticCount] != null) {
                    if (observation.values[statisticCount].getClass() != type.getValueClass()) {
                        return false;
                    }
                    statisticCount++;
                }

            }
        }
        // check that there aren't extra observation values
        if (statisticCount == observation.values.length) {
            return true;
        }
        else {
            return false;
        }
    }

    private boolean addToAttributeMap(String varName, String attrName,
                                      Object attrValue) {
        List[] values = attributeMap.get(attrName);
        if (null == values) {
            values = (LinkedList[]) new LinkedList[2];
            values[0] = new LinkedList();
            values[1] = new LinkedList();
            attributeMap.put(attrName, values);
        }
        return values[0].add(varName) && values[1].add(attrValue);
    }
}
