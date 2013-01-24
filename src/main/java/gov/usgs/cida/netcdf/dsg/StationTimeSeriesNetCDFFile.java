package gov.usgs.cida.netcdf.dsg;

import java.nio.ByteBuffer;
import java.io.Closeable;
import com.sun.jna.NativeLong;
import com.sun.jna.ptr.IntByReference;
import com.sun.jna.ptr.NativeLongByReference;
import java.io.File;
import static gov.usgs.cida.netcdf.jna.NC.*;
import static gov.usgs.cida.netcdf.jna.NCUtil.*;
import java.nio.ByteOrder;
import java.util.Map;

/**
 *
 * @author Jordan Walker <jiwalker@usgs.gov>
 */
public class StationTimeSeriesNetCDFFile implements Closeable {
    
    private static final int STRING_LENGTH = 32;

    private static final short _FillValue_SHORT = -9999;

    private final static String STATION_DIM_NAME = "station";
    private final static String STATION_ID_LEN_NAME = "station_id_len";
    protected final static String OBSERVATION_DIM_NAME = "observation";
    protected final static String OBSERVATION_STRUCT_NAME = "record"; // NetCDF-Java reqiures this to be record (last tested release was 4.2.26)

    public final String name;
    public final int createFlags;
    public final int ncId;
    public final int ncDimId_station;
    public final int ncDimId_observation;
    public final int ncDimId_station_id_len;
    
    private RecordType record;
    private int record_index;
    private boolean isClosed = false;

    /**
     * Put this in place to keep the interface the same for anything using this currently
     * @param file Output NetCDF file
     * @param rt RecordType matching the 
     * @param doChunking
     * @param stations 
     */
    public StationTimeSeriesNetCDFFile(File file, RecordType rt, boolean doChunking, Station ... stations) {
        this(file, rt, null, doChunking, stations);
    }
    
    public StationTimeSeriesNetCDFFile(File file, RecordType rt, Map<String,String> globalAttrs,
            boolean doChunking, Station ... stations) {
        this.record = rt;
        this.record_index = 0;
        this.name = file.getName();

        this.createFlags = NC_NETCDF4;

        //this.convention = convention;

        int ncStatus;
        IntByReference iRef = new IntByReference();

        ncStatus = nc_create(file.getAbsolutePath(), createFlags, iRef); status(ncStatus);
        ncId = iRef.getValue();

        // DIMENSIONS:
        ncStatus = nc_def_dim(ncId, STATION_DIM_NAME, new NativeLong(stations.length), iRef); status(ncStatus);
        ncDimId_station = iRef.getValue();
        
        int max_length = Station.findMaxStationLength(stations);
        ncStatus = nc_def_dim(ncId, STATION_ID_LEN_NAME, new NativeLong(max_length), iRef); status(ncStatus);
        ncDimId_station_id_len = iRef.getValue();
        
        ncStatus = nc_def_dim(ncId, OBSERVATION_DIM_NAME, new NativeLong(NC_UNLIMITED), iRef); status(ncStatus);
        ncDimId_observation = iRef.getValue();
        
        //// VARIABLES
        // STATION
        int ncTypeId_record_type = this.record.writeRecordCompound(ncId);
        Map<String, Variable> stVars = this.record.writeStationVariables(ncId, ncDimId_station, ncDimId_station_id_len);
        this.record.writeObservationVariables(ncId, ncDimId_observation, ncTypeId_record_type, doChunking);
        
        // Global Attributes
        this.record.writeGlobalAttributes(ncId, globalAttrs); // expose to allow additional global attributes
        
        ncStatus = nc_enddef(ncId); status(ncStatus);
        
        // Now lets fill in the Station data from vararg stations
        // Then we're set up to start putting observations
        NativeLong station_indexp = new NativeLong(0);
        NativeLong stationid_len_indexp = new NativeLong(0);
        for (Station station : stations) {
            ncStatus = nc_put_var1_float(ncId, stVars.get("lon").ncVarId, station.longitude, station_indexp); status(ncStatus);
            ncStatus = nc_put_var1_float(ncId, stVars.get("lat").ncVarId, station.latitude, station_indexp); status(ncStatus);
            ncStatus = nc_put_vara_text(ncId, stVars.get("station_id").ncVarId, station.station_id, station_indexp, stationid_len_indexp); status(ncStatus);
            station_indexp.setValue(station_indexp.longValue() + 1);
        }
    }

    public boolean putObservation(Observation observation) {
        int ncStatus = 0;
        NativeLongByReference record_startp = new NativeLongByReference(new NativeLong(record_index));
        NativeLongByReference record_countp = new NativeLongByReference(new NativeLong(1));
        
        boolean observationPutSuccessful = false;
        if (!isClosed && record.isObservationValid(observation)) {
            ByteBuffer recordBuffer = ByteBuffer.allocateDirect(record.getCompoundSize());
            recordBuffer.order(ByteOrder.nativeOrder());
            
            recordBuffer.putInt(observation.station_index);
            recordBuffer.putInt(observation.time_offset);
            for (Object value : observation.values) {
                if (value instanceof String) {
                    throw new UnsupportedOperationException(
                            "Need to implement char[] or string");
//                    String val = (String) value;
//                    ncStatus = nc_put_att_text(ncId, ncVarId, key, val);
//                    status(ncStatus);
                }
                else if (value instanceof Integer) {
                    int val = ((Integer) value).intValue();
                    recordBuffer.putInt(val);
                }
                else if (value instanceof Short) {
                    short val = ((Short) value).shortValue();
                    recordBuffer.putShort(val);
                }
                else if (value instanceof Float) {
                    float val = ((Float) value).floatValue();
                    recordBuffer.putFloat(val);
                }
                else if (value instanceof Double) {
                    double val = ((Double) value).doubleValue();
                    recordBuffer.putDouble(val);
                }
                else {
                    throw new UnsupportedOperationException(
                            "Should implement the other types");
                }
            } // record complete
            
            recordBuffer.rewind();
            ncStatus = nc_put_vara(ncId, record.getVarId(), record_startp, record_countp, recordBuffer); status(ncStatus);
            record_index++;
            
            //station_indexp.setValue(station_indexp.longValue() + 1);
            return true;
        }
        return observationPutSuccessful;
    }

    public void close() {
        status(nc_close(ncId));
        isClosed = true;
    }

    public void sync() {
        status(nc_sync(ncId));
    }
}

