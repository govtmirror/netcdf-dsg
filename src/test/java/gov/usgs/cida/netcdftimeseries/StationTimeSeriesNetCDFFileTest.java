package gov.usgs.cida.netcdftimeseries;

import edu.unidata.ucar.netcdf.jna.NCUtil.XType;
import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;
import junit.framework.TestCase;

/**
 *
 * @author Jordan Walker <jiwalker@usgs.gov>
 */
public class StationTimeSeriesNetCDFFileTest extends TestCase {
    
    private File testfile;
    
    public StationTimeSeriesNetCDFFileTest(String testName) {
        super(testName);
    }
    
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        testfile = new File("/tmp/test.nc");
    }
    
    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        testfile.delete();
    }

    /**
     * Test of putObservation method, of class StationTimeSeriesNetCDFFile.
     */
    public void testNoObservations() {
        
        File file = testfile;
        //List<Station> stationList = new LinkedList<Station>();
        Station station1 = new Station(40.0f, -89.3f, "station_test");
        Station station2 = new Station(-34f, 44.6f, "another_station");
        RecordType rt = new RecordType("days since 2011-01-01 00:00:00Z");
        Map<String, Object> attrMap = new LinkedHashMap<String, Object>();
        attrMap.put("standard_name", "mean_days_above_temperature_threshold");
        // TODO don't allow _FileValue as it breaks netcdf, switch to missing_value?
        //attrMap.put("_FillValue", new Float(-999.99f));
        attrMap.put("units", "days");
        rt.addType(new Variable("mean", XType.NC_FLOAT, attrMap));
        StationTimeSeriesNetCDFFile instance = new StationTimeSeriesNetCDFFile(
                file, rt, true, station1, station2);
        
        instance.close();
        assertTrue(file.exists());
    }
    
    /**
     * Test of putObservation method, of class StationTimeSeriesNetCDFFile.
     */
    public void testNoObservationsAgain() {
        
        File file = testfile;
        //List<Station> stationList = new LinkedList<Station>();
        Station station1 = new Station(40.0f, -89.3f, "station_test");
        Station station2 = new Station(-34f, 44.6f, "another_station");
        RecordType rt = new RecordType("days since 2011-01-01 00:00:00Z");
        Map<String, Object> attrMap = new LinkedHashMap<String, Object>();
        attrMap.put("standard_name", "mean_days_above_temperature_threshold");
        // TODO don't allow _FileValue as it breaks netcdf, switch to missing_value?
        //attrMap.put("_FillValue", new Float(-999.99f));
        attrMap.put("units", "days");
        rt.addType(new Variable("mean", XType.NC_FLOAT, attrMap));
        attrMap = new LinkedHashMap<String, Object>();
        attrMap.put("standard_name", "minimum_days_above_temperature_threshold");
        // TODO don't allow _FileValue as it breaks netcdf, switch to missing_value?
        //attrMap.put("_FillValue", new Float(-999.99f));
        attrMap.put("units", "days");
        rt.addType(new Variable("min", XType.NC_INT, attrMap));
        
        StationTimeSeriesNetCDFFile instance = new StationTimeSeriesNetCDFFile(
                file, rt, true, station1, station2);
        
        instance.close();
        assertTrue(file.exists());
    }
    
    public void testTomsCDL() {
        File file = testfile;
        //List<Station> stationList = new LinkedList<Station>();
        Station station1 = new Station(41f, -109f, "demoHUCs.1");
        Station station2 = new Station(40f, -107f, "demoHUCs.2");
        RecordType rt = new RecordType("days since 2000-01-01 00:00:00");
        Map<String, Object> attrMap = new LinkedHashMap<String, Object>();
        attrMap.put("units", "degC");
        rt.addType(new Variable("min", XType.NC_FLOAT, attrMap));
        rt.addType(new Variable("max", XType.NC_FLOAT, attrMap));
        rt.addType(new Variable("mean", XType.NC_FLOAT, attrMap));
        
        StationTimeSeriesNetCDFFile instance = new StationTimeSeriesNetCDFFile(
                file, rt, true, station1, station2);
        for (int time=0; time<=9; time++) {
            for (int index=0; index<=1; index++) {
                int val = time + (index*10);
                Float min = new Float(val);
                Float max = new Float(val+10);
                Float mean = new Float(val+5);
                
                assertTrue(instance.putObservation(new Observation(time, index, min, max, mean)));
            }
        }
        
        instance.close();
        assertTrue(file.exists());
    }
}
