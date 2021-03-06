/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package language.detector.examples.processors;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import language.detector.examples.processors.LanguageDetectorMain;

import static org.junit.Assert.*;

/**
 *
 * @author phillip
 */
public class LanguageDetectorMainTest {
    
    /**
     * Test of onTrigger method, of class JsonProcessor.
     */
    @org.junit.Test
    public void testOnTrigger() throws IOException {
      /*  // Content to be mock a json file
        InputStream content = new ByteArrayInputStream("{\"hello\":\"nifi rocks\"}".getBytes());
        
        // Generate a test runner to mock a processor in a flow
        TestRunner runner = TestRunners.newTestRunner(new LanguageDetectorMain());
        
        // Add properties
        runner.setProperty(LanguageDetectorMain.ALPHA, "0.5");
        
        // Add the content to the runner
        runner.enqueue(content);
        
        // Run the enqueued content, it also takes an int = number of contents queued
        runner.run(1);
        
        // All results were processed with out failure
        runner.assertQueueEmpty();
        
        // If you need to read or do additional tests on results you can access the content
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(LanguageDetectorMain.SUCCESS);
        assertTrue("1 match", results.size() == 1);
        MockFlowFile result = results.get(0);
        String resultValue = new String(runner.getContentAsByteArray(result));
        System.out.println("Match: " + IOUtils.toString(runner.getContentAsByteArray(result)));
        
        // Test attributes and content
       /* result.assertAttributeEquals(LanguageDetectorMain., "nifi rocks");
        result.assertContentEquals("nifi rocks");*/
        
    }
    
}
