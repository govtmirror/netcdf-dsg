package gov.usgs.cida.netcdf.dsg;



import gov.usgs.cida.netcdf.dsg.*;
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
public class StationTimeSeriesMultiDimensionalTest {
    
    private File testfile;
    
    @Before
    public void setUp() throws Exception {
        testfile = new File("/tmp/test.nc");
    }
    
    @After
    public void tearDown() throws Exception {
        testfile.delete();
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

    /**
     * Test of putObservation method, of class StationTimeSeriesNetCDFFile.
     */
    @Test
    public void testSimple() throws IOException {
        
        File file = testfile;
        Station station1 = new Station(40.0f, -89.3f, "station_test1");
        Station station2 = new Station(-34f, 44.6f, "station_test2");
        Map<String, Object> attrMap = new LinkedHashMap<String, Object>();
        attrMap.put("standard_name", "mean_days_above_temperature_threshold");
        attrMap.put("units", "days");
        StationTimeSeriesMultiDimensional instance = new StationTimeSeriesMultiDimensional(
                file,
                null,
                new Station[] { station1, station2 },
                new int[] { 0, 1 },
                "days since 2011-01-01 00:00:00Z",
                new Variable[] {
                    new Variable("min", XType.NC_FLOAT, attrMap),
                    new Variable("mean", XType.NC_FLOAT, attrMap),
                    new Variable("max", XType.NC_FLOAT, attrMap) });
        // time, station, data
        instance.putObservation(new Observation(0, 0, 1f, 2f, 3f));
        instance.putObservation(new Observation(1, 0, 2f, 3f, 4f));
        // time, station, data
        instance.putObservation(new Observation(0, 1, 3f, 4f, 5f));
        instance.putObservation(new Observation(1, 1, 4f, 5f, 6f));
        
        instance.close();
        assertTrue(file.exists());
        validateNetCDFFileAsDSG(file);
    }
    

}
