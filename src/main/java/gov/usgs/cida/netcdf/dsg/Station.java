package gov.usgs.cida.netcdf.dsg;

/**
 *
 * @author Jordan Walker <jiwalker@usgs.gov>
 */
public class Station {
    public final float latitude;
    public final float longitude;
    public final String station_id;
    // for convenience, used to save from doing multiple lookups
    // can be changed or ignored based on the scheme for writing to file
    public int index;
    
    public Station(float lat, float lon, String station_id) {
        this(lat, lon, station_id, -1);
    }
    
    public Station(float lat, float lon, String station_id, int index) {
        this.latitude = lat;
        this.longitude = lon;
        this.station_id = station_id;
        this.index = index;
    }
    
    /*
     * wanted to use static max variable, but need to keep it for list of stations
     */
    public static int findMaxStationLength(Station... stations) {
        int max = 0;
        for (Station x : stations) {
            int len = x.station_id.length();
            if (len > max) {
                max = len;
            }
        }
        return max;
    }
    
}
