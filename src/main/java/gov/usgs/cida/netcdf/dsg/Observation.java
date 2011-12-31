package gov.usgs.cida.netcdf.dsg;
/**
 *
 * @author Jordan Walker <jiwalker@usgs.gov>
 */
public class Observation {
    
    public final int time_offset;
    public final int station_index;
    public final Object[] values;
    
    /*
     * Take time as an offset from some time
     * TODO take a DateTime from Joda and convert it to index here or in record
     */
    public Observation(int time, int index, Object... values) {
        this.time_offset = time;
        this.station_index = index;
        this.values = values;
    } 
}
