package gov.usgs.cida.netcdftimeseries;

import edu.unidata.ucar.netcdf.jna.NCUtil.XType;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author Jordan Walker <jiwalker@usgs.gov>
 */
public class Variable {
    public final String name;
    public final VariableType vtype;
    public final XType xtype;
    public final Map<String, Object> attributes;
    public int ncVarId;
            
    enum VariableType {
        TIME,
        STATION_ID,
        LAT_LON,
        STATISTIC
    }
    
    public Variable(String name, XType xtype, Map<String, Object> attrs) {
        this.name = name;
        this.attributes = attrs;
        this.xtype = xtype;
        if (name.equals("time")) {
            vtype = VariableType.TIME;
        } else if (name.equals("station_id")) {
            vtype = VariableType.STATION_ID;
        } else if (name.equals("lat") || name.equals("lon")) {
            vtype = VariableType.LAT_LON;
        } else {
            vtype = VariableType.STATISTIC;
        }
        
        ncVarId = -1;  // This should be updated when this Variable object is used to call nc_def_var
    }
    
    public Class getValueClass() {
        switch (this.xtype) {
            case NC_BYTE:
                return Byte.class;
            case NC_DOUBLE:
                return Double.class;
            case NC_FLOAT:
                return Float.class;
            case NC_INT:
                return Integer.class;
            case NC_LONG:
                return Long.class;
            case NC_SHORT:
                return Short.class;
            case NC_STRING:
            case NC_CHAR:
                return String.class;
            default:
                return Error.class;
        }
    }
    
    // Quick constructor for time
    public static Variable createTimeVariable(String timeUnits) {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("units", timeUnits);
        map.put("standard_name", "time");
        return new Variable("time", XType.NC_INT, map);
    }
    
    // Quick constructor for station_id
    public static Variable createStationIdVariable() {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("standard_name", "station_id");
        map.put("cf_role", "timeseries_id");
        return new Variable("station_id", XType.NC_INT, map);
    }
    
    public static Variable createLatitude() {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("standard_name", "latitude");
        map.put("units", "degrees_east");
        return new Variable("lat", XType.NC_FLOAT, map);
    }
    
    public static Variable createLongitude() {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("standard_name", "longitude");
        map.put("units", "degrees_north");
        return new Variable("lon", XType.NC_FLOAT, map);
    }
}
