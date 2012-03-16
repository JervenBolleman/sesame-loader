/**
 * 
 */
package com.github.sesameloader.test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;

import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.UnsupportedRDFormatException;
import org.openrdf.sail.SailException;

import com.github.sesameloader.LoaderMain;
import com.github.sesameloader.RepositoryManager;

/**
 * @author Peter Ansell p_ansell@yahoo.com
 * 
 */
public class LoaderMainTest
{
    
    /**
     * JUnit creates this temporary folder before each test and cleans up after each test.
     * 
     * All of the test files are located inside of subfolders within folder, so they should all be
     * cleaned up by JUnit.
     */
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    
    private File repositoryFolder;
    
    private File testDataFolder;
    
    private File testDataFileRdf;
    
    private File testDataFileN3;
    
    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        // create a temporary folder for our test data, which is populated based on resources in the
        // test jar file
        this.testDataFolder = this.folder.newFolder();
        
        // create a randomly named temporary file in RDF/XML format
        this.testDataFileRdf = File.createTempFile("loadermaintest-1-", ".rdf", this.testDataFolder);
        final FileOutputStream testOutputStreamRdf = new FileOutputStream(this.testDataFileRdf);
        final InputStream testResource1 = this.getClass().getResourceAsStream("loadermaintest-1.rdf");
        
        Assert.assertNotNull("Test resource not found", testResource1);
        
        IOUtils.copy(testResource1, testOutputStreamRdf);
        
        // create a randomly named temporary file in N3 format
        this.testDataFileN3 = File.createTempFile("loadermaintest-1-", ".n3", this.testDataFolder);
        final FileOutputStream testOutputStreamN3 = new FileOutputStream(this.testDataFileN3);
        final InputStream testResource2 = this.getClass().getResourceAsStream("loadermaintest-1.n3");
        
        Assert.assertNotNull("Test resource not found", testResource2);
        
        IOUtils.copy(testResource2, testOutputStreamN3);
        
        // create a separate folder for the repository data
        this.repositoryFolder = this.folder.newFolder();
        
    }
    
    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
    }
    
    /**
     * Test method for
     * {@link com.github.sesameloader.test.LoaderMain#getRepositoryManager(java.io.File, java.lang.String)}
     * .
     * 
     * @throws SailException
     * @throws RepositoryException
     */
    @Test
    public void testGetRepositoryManagerNative() throws RepositoryException, SailException
    {
        final RepositoryManager nativeRepositoryManager =
                LoaderMain.getRepositoryManager(this.repositoryFolder, "native");
        
        Assert.assertNotNull(nativeRepositoryManager);
    }
    
    /**
     * Test method for
     * {@link com.github.sesameloader.test.LoaderMain#LoaderMain(java.io.File, java.lang.String, java.lang.Integer, java.lang.Integer)}
     * .
     * 
     * @throws RepositoryException
     * @throws SailException
     */
    @Test
    public void testLoaderMainFileStringIntegerInteger() throws URISyntaxException, SailException, RepositoryException
    {
        final LoaderMain loader = new LoaderMain(this.repositoryFolder, "native", new Integer(10), new Integer(5));
    }
    
    /**
     * Test method for
     * {@link com.github.sesameloader.test.LoaderMain#LoaderMain(com.github.sesameloader.test.RepositoryManager, java.lang.Integer, java.lang.Integer)}
     * .
     * 
     * @throws RepositoryException
     * @throws SailException
     */
    @Test
    public void testLoaderMainRepositoryManagerIntegerInteger() throws SailException, RepositoryException
    {
        final LoaderMain loader =
                new LoaderMain(LoaderMain.getRepositoryManager(this.repositoryFolder, "native"), new Integer(20),
                        new Integer(10));
    }
    
    /**
     * Test method for
     * {@link com.github.sesameloader.test.LoaderMain#load(java.io.File, java.lang.String)}.
     * 
     * @throws RepositoryException
     * @throws SailException
     * @throws IOException
     * @throws FileNotFoundException
     */
    @Test
    public void testLoadFileNativeDirectoryMixed() throws SailException, RepositoryException, FileNotFoundException,
        IOException
    {
        final RepositoryManager repositoryManagerBefore =
                LoaderMain.getRepositoryManager(this.repositoryFolder, "native");
        
        RepositoryConnection beforeConnection = null;
        
        try
        {
            beforeConnection = repositoryManagerBefore.getConnection();
            Assert.assertEquals(0, beforeConnection.size());
        }
        finally
        {
            if(beforeConnection != null)
            {
                beforeConnection.close();
            }
            repositoryManagerBefore.shutDown();
        }
        
        final RepositoryManager repositoryManager = LoaderMain.getRepositoryManager(this.repositoryFolder, "native");
        
        final LoaderMain loader = new LoaderMain(repositoryManager, new Integer(20), new Integer(10));
        
        loader.load(this.testDataFolder, "http://test.example.org/test/load/file/native/rdf/base/uri");
        
        repositoryManager.shutDown();
        
        // NOTE: LoaderMain automatically shuts down the repository after a single call to load
        
        // Therefore, it is okay to create another repository manager here
        
        final RepositoryManager repositoryManagerAfter =
                LoaderMain.getRepositoryManager(this.repositoryFolder, "native");
        
        RepositoryConnection afterConnection = null;
        
        try
        {
            afterConnection = repositoryManagerAfter.getConnection();
            
            Assert.assertTrue(afterConnection.size() > 0);
        }
        finally
        {
            if(afterConnection != null)
            {
                afterConnection.close();
            }
            
            repositoryManagerAfter.shutDown();
        }
    }
    
    /**
     * Test method for
     * {@link com.github.sesameloader.test.LoaderMain#load(java.io.File, java.lang.String)}.
     * 
     * @throws RepositoryException
     * @throws SailException
     * @throws IOException
     * @throws FileNotFoundException
     */
    @Test
    public void testLoadFileNativeN3() throws SailException, RepositoryException, FileNotFoundException, IOException
    {
        final RepositoryManager repositoryManagerBefore =
                LoaderMain.getRepositoryManager(this.repositoryFolder, "native");
        
        RepositoryConnection beforeConnection = null;
        
        try
        {
            beforeConnection = repositoryManagerBefore.getConnection();
            Assert.assertEquals(0, beforeConnection.size());
        }
        finally
        {
            if(beforeConnection != null)
            {
                beforeConnection.close();
            }
            repositoryManagerBefore.shutDown();
        }
        
        final RepositoryManager repositoryManager = LoaderMain.getRepositoryManager(this.repositoryFolder, "native");
        
        final LoaderMain loader = new LoaderMain(repositoryManager, new Integer(20), new Integer(10));
        
        loader.load(this.testDataFileN3, "http://test.example.org/test/load/file/native/rdf/base/uri");
        
        repositoryManager.shutDown();
        
        // NOTE: LoaderMain automatically shuts down the repository after a single call to load
        
        // Therefore, it is okay to create another repository manager here
        
        final RepositoryManager repositoryManagerAfter =
                LoaderMain.getRepositoryManager(this.repositoryFolder, "native");
        
        RepositoryConnection afterConnection = null;
        
        try
        {
            afterConnection = repositoryManagerAfter.getConnection();
            
            Assert.assertTrue(afterConnection.size() > 0);
        }
        finally
        {
            if(afterConnection != null)
            {
                afterConnection.close();
            }
            
            repositoryManagerAfter.shutDown();
        }
    }
    
    /**
     * Test method for
     * {@link com.github.sesameloader.test.LoaderMain#load(java.io.File, java.lang.String)}.
     * 
     * @throws RepositoryException
     * @throws SailException
     * @throws IOException
     * @throws FileNotFoundException
     */
    @Test
    public void testLoadFileNativeRdf() throws SailException, RepositoryException, FileNotFoundException, IOException
    {
        final RepositoryManager repositoryManagerBefore =
                LoaderMain.getRepositoryManager(this.repositoryFolder, "native");
        
        RepositoryConnection beforeConnection = null;
        
        try
        {
            beforeConnection = repositoryManagerBefore.getConnection();
            Assert.assertEquals(0, beforeConnection.size());
        }
        finally
        {
            if(beforeConnection != null)
            {
                beforeConnection.close();
            }
            repositoryManagerBefore.shutDown();
        }
        
        final RepositoryManager repositoryManager = LoaderMain.getRepositoryManager(this.repositoryFolder, "native");
        
        final LoaderMain loader = new LoaderMain(repositoryManager, new Integer(20), new Integer(10));
        
        loader.load(this.testDataFileRdf, "http://test.example.org/test/load/file/native/rdf/base/uri");
        
        repositoryManager.shutDown();
        
        // NOTE: LoaderMain automatically shuts down the repository after a single call to load
        
        // Therefore, it is okay to create another repository manager here
        
        final RepositoryManager repositoryManagerAfter =
                LoaderMain.getRepositoryManager(this.repositoryFolder, "native");
        
        RepositoryConnection afterConnection = null;
        
        try
        {
            afterConnection = repositoryManagerAfter.getConnection();
            
            Assert.assertTrue(afterConnection.size() > 0);
        }
        finally
        {
            if(afterConnection != null)
            {
                afterConnection.close();
            }
            
            repositoryManagerAfter.shutDown();
        }
    }
    
    /**
     * Test method for
     * {@link com.github.sesameloader.test.LoaderMain#load(java.io.InputStream, org.openrdf.rio.RDFFormat, java.lang.String)}
     * .
     * 
     * @throws SailException
     * @throws RepositoryException
     * @throws IOException
     * @throws FileNotFoundException
     * @throws UnsupportedRDFormatException
     * @throws RDFHandlerException
     * @throws RDFParseException
     */
    @Test
    public void testLoadInputStreamN3() throws RepositoryException, SailException, FileNotFoundException, IOException,
        RDFParseException, RDFHandlerException, UnsupportedRDFormatException
    {
        final RepositoryManager repositoryManagerBefore =
                LoaderMain.getRepositoryManager(this.repositoryFolder, "native");
        
        RepositoryConnection beforeConnection = null;
        
        try
        {
            beforeConnection = repositoryManagerBefore.getConnection();
            Assert.assertEquals(0, beforeConnection.size());
        }
        finally
        {
            if(beforeConnection != null)
            {
                beforeConnection.close();
            }
            repositoryManagerBefore.shutDown();
        }
        
        final RepositoryManager repositoryManager = LoaderMain.getRepositoryManager(this.repositoryFolder, "native");
        
        final LoaderMain loader = new LoaderMain(repositoryManager, new Integer(20), new Integer(10));
        
        final InputStream testResource1 = this.getClass().getResourceAsStream("loadermaintest-1.n3");
        
        loader.load(testResource1, RDFFormat.N3, "http://test.example.org/test/load/file/native/rdf/base/uri");
        
        repositoryManager.shutDown();
        
        // NOTE: LoaderMain automatically shuts down the repository after a single call to load
        
        // Therefore, it is okay to create another repository manager here
        
        final RepositoryManager repositoryManagerAfter =
                LoaderMain.getRepositoryManager(this.repositoryFolder, "native");
        
        RepositoryConnection afterConnection = null;
        
        try
        {
            afterConnection = repositoryManagerAfter.getConnection();
            
            Assert.assertTrue(afterConnection.size() > 0);
        }
        finally
        {
            if(afterConnection != null)
            {
                afterConnection.close();
            }
            
            repositoryManagerAfter.shutDown();
        }
        
    }
    
    /**
     * Test method for
     * {@link com.github.sesameloader.test.LoaderMain#load(java.io.InputStream, org.openrdf.rio.RDFFormat, java.lang.String)}
     * .
     * 
     * @throws SailException
     * @throws RepositoryException
     * @throws IOException
     * @throws FileNotFoundException
     * @throws UnsupportedRDFormatException
     * @throws RDFHandlerException
     * @throws RDFParseException
     */
    @Test
    public void testLoadInputStreamRdf() throws RepositoryException, SailException, FileNotFoundException, IOException,
        RDFParseException, RDFHandlerException, UnsupportedRDFormatException
    {
        final RepositoryManager repositoryManagerBefore =
                LoaderMain.getRepositoryManager(this.repositoryFolder, "native");
        
        RepositoryConnection beforeConnection = null;
        
        try
        {
            beforeConnection = repositoryManagerBefore.getConnection();
            Assert.assertEquals(0, beforeConnection.size());
        }
        finally
        {
            if(beforeConnection != null)
            {
                beforeConnection.close();
            }
            repositoryManagerBefore.shutDown();
        }
        
        final RepositoryManager repositoryManager = LoaderMain.getRepositoryManager(this.repositoryFolder, "native");
        
        final LoaderMain loader = new LoaderMain(repositoryManager, new Integer(20), new Integer(10));
        
        final InputStream testResource1 = this.getClass().getResourceAsStream("loadermaintest-1.rdf");
        
        loader.load(testResource1, RDFFormat.RDFXML, "http://test.example.org/test/load/file/native/rdf/base/uri");
        
        repositoryManager.shutDown();
        
        // NOTE: LoaderMain automatically shuts down the repository after a single call to load
        
        // Therefore, it is okay to create another repository manager here
        
        final RepositoryManager repositoryManagerAfter =
                LoaderMain.getRepositoryManager(this.repositoryFolder, "native");
        
        RepositoryConnection afterConnection = null;
        
        try
        {
            afterConnection = repositoryManagerAfter.getConnection();
            
            Assert.assertTrue(afterConnection.size() > 0);
        }
        finally
        {
            if(afterConnection != null)
            {
                afterConnection.close();
            }
            
            repositoryManagerAfter.shutDown();
        }
        
    }
    
    @Test
    public void testMaxThreadsFailure() throws SailException, RepositoryException
    {
        final String expectedExceptionMessage =
                "Tried to select more than the maximum number of threads for the given repository manager";
        
        final RepositoryManager nextManager = new MaxOneThreadRepositoryManager();
        
        try
        {
            final LoaderMain loader = new LoaderMain(nextManager, 100, 2);
            Assert.fail("Did not receive expected exception");
        }
        catch(final RuntimeException rex)
        {
            Assert.assertTrue(rex.getMessage().contains(expectedExceptionMessage));
        }
    }
    
    @Test
    public void testMaxThreadsSuccess() throws SailException, RepositoryException
    {
        final String expectedExceptionMessage =
                "Tried to select more than the maximum number of threads for the given repository manager";
        
        final RepositoryManager nextManager = new MaxOneThreadRepositoryManager();
        
        final LoaderMain loader = new LoaderMain(nextManager, 100, 1);
    }
    
}
