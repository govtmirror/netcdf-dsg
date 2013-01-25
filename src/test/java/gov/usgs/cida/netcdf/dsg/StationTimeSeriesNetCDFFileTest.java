package gov.usgs.cida.netcdf.dsg;

import gov.usgs.cida.netcdf.jna.NCUtil.XType;
import java.io.File;
import java.io.IOException;
import java.util.Formatter;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.After;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;
import ucar.nc2.constants.FeatureType;
import ucar.nc2.ft.FeatureDataset;
import ucar.nc2.ft.FeatureDatasetFactoryManager;

/**
 *
 * @author Jordan Walker <jiwalker@usgs.gov>
 */
public class StationTimeSeriesNetCDFFileTest {
    
    private File testfile;
    
    @Before
    public void setUp() throws Exception {
        testfile = new File("/tmp/test.nc");
    }
    
    @After
    public void tearDown() throws Exception {
        testfile.delete();
    }

    /**
     * Test of putObservation method, of class StationTimeSeriesNetCDFFile.
     */
    @Test
    public void testSimple_RaggedIndex_StationOuter() throws IOException {
        
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
        
        instance.putObservation(new Observation(0, 0, 1f));
        instance.putObservation(new Observation(1, 0, 2f));
        
        instance.putObservation(new Observation(0, 1, 3f));
        instance.putObservation(new Observation(1, 1, 4f));
        
        instance.close();
        assertTrue(file.exists());
        validateNetCDFFileAsDSG(file);
    }
    
    /**
     * Test of putObservation method, of class StationTimeSeriesNetCDFFile.
     */
    @Test
    public void testSimple_RaggedIndex_TimeOuter() throws IOException {
        
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
        
        instance.putObservation(new Observation(0, 0, 1f));
        instance.putObservation(new Observation(0, 1, 2f));
        
        instance.putObservation(new Observation(1, 0, 3f));
        instance.putObservation(new Observation(1, 1, 4f));
        
        instance.close();
        assertTrue(file.exists());
        validateNetCDFFileAsDSG(file);
    }
    
    @Test
    public void testTomsCDL() throws IOException {
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
        validateNetCDFFileAsDSG(file);
    }
    
    private void validateNetCDFFileAsDSG(File file) throws IOException {
        String path = file.getAbsolutePath();
        FeatureDataset fds = null;
        try {
            fds = FeatureDatasetFactoryManager.open(FeatureType.ANY, path, null, new Formatter(System.err));
            assertNotNull("Unable to open " + path, fds);
            assertEquals("NetCDF file not recognized as CF 1.6 DSG", fds.getFeatureType(), FeatureType.STATION);
            fds.getNetcdfFile().writeCDL(System.out, true);
        } finally {
            if (fds != null) {
                try { fds.close(); } catch (IOException ignore) { }
            }
        }
    }
}
