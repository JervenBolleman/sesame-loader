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
        testDataFolder = folder.newFolder();
        
        // create a randomly named temporary file in RDF/XML format
        testDataFileRdf = File.createTempFile("loadermaintest-1-", ".rdf", testDataFolder);
        FileOutputStream testOutputStreamRdf = new FileOutputStream(testDataFileRdf);
        InputStream testResource1 = this.getClass().getResourceAsStream("loadermaintest-1.rdf");
        
        Assert.assertNotNull("Test resource not found", testResource1);
        
        IOUtils.copy(testResource1, testOutputStreamRdf);
        
        // create a randomly named temporary file in N3 format
        testDataFileN3 = File.createTempFile("loadermaintest-1-", ".n3", testDataFolder);
        FileOutputStream testOutputStreamN3 = new FileOutputStream(testDataFileN3);
        InputStream testResource2 = this.getClass().getResourceAsStream("loadermaintest-1.n3");
        
        Assert.assertNotNull("Test resource not found", testResource2);
        
        IOUtils.copy(testResource2, testOutputStreamN3);
        
        // create a separate folder for the repository data
        repositoryFolder = folder.newFolder();
        
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
     * {@link com.github.sesameloader.test.LoaderMain#LoaderMain(java.io.File, java.lang.String, java.lang.Integer, java.lang.Integer)}
     * .
     * 
     * @throws RepositoryException
     * @throws SailException
     */
    @Test
    public void testLoaderMainFileStringIntegerInteger() throws URISyntaxException, SailException, RepositoryException
    {
        LoaderMain loader = new LoaderMain(repositoryFolder, "native", new Integer(10), new Integer(5));
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
        LoaderMain loader =
                new LoaderMain(LoaderMain.getRepositoryManager(repositoryFolder, "native"), new Integer(20),
                        new Integer(10));
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
        RepositoryManager nativeRepositoryManager = LoaderMain.getRepositoryManager(repositoryFolder, "native");
        
        Assert.assertNotNull(nativeRepositoryManager);
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
        RepositoryManager repositoryManagerBefore = LoaderMain.getRepositoryManager(repositoryFolder, "native");
        
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
        
        RepositoryManager repositoryManager = LoaderMain.getRepositoryManager(repositoryFolder, "native");
        
        LoaderMain loader =
                new LoaderMain(repositoryManager, new Integer(20),
                        new Integer(10));
        
        loader.load(testDataFileRdf, "http://test.example.org/test/load/file/native/rdf/base/uri");
        
        repositoryManager.shutDown();
        
        // NOTE: LoaderMain automatically shuts down the repository after a single call to load
        
        // Therefore, it is okay to create another repository manager here
        
        RepositoryManager repositoryManagerAfter = LoaderMain.getRepositoryManager(repositoryFolder, "native");
        
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
    public void testLoadFileNativeDirectoryMixed() throws SailException, RepositoryException, FileNotFoundException, IOException
    {
        RepositoryManager repositoryManagerBefore = LoaderMain.getRepositoryManager(repositoryFolder, "native");
        
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
        
        RepositoryManager repositoryManager = LoaderMain.getRepositoryManager(repositoryFolder, "native");
        
        LoaderMain loader =
                new LoaderMain(repositoryManager, new Integer(20),
                        new Integer(10));
        
        loader.load(testDataFolder, "http://test.example.org/test/load/file/native/rdf/base/uri");
        
        repositoryManager.shutDown();
        
        // NOTE: LoaderMain automatically shuts down the repository after a single call to load
        
        // Therefore, it is okay to create another repository manager here
        
        RepositoryManager repositoryManagerAfter = LoaderMain.getRepositoryManager(repositoryFolder, "native");
        
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
        RepositoryManager repositoryManagerBefore = LoaderMain.getRepositoryManager(repositoryFolder, "native");
        
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
        
        RepositoryManager repositoryManager = LoaderMain.getRepositoryManager(repositoryFolder, "native");
        
        LoaderMain loader =
                new LoaderMain(repositoryManager, new Integer(20),
                        new Integer(10));
        
        InputStream testResource1 = this.getClass().getResourceAsStream("loadermaintest-1.rdf");
        
        loader.load(testResource1, RDFFormat.RDFXML, "http://test.example.org/test/load/file/native/rdf/base/uri");
        
        repositoryManager.shutDown();
        
        // NOTE: LoaderMain automatically shuts down the repository after a single call to load
        
        // Therefore, it is okay to create another repository manager here
        
        RepositoryManager repositoryManagerAfter = LoaderMain.getRepositoryManager(repositoryFolder, "native");
        
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
        RepositoryManager repositoryManagerBefore = LoaderMain.getRepositoryManager(repositoryFolder, "native");
        
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
        
        RepositoryManager repositoryManager = LoaderMain.getRepositoryManager(repositoryFolder, "native");
        
        LoaderMain loader =
                new LoaderMain(repositoryManager, new Integer(20),
                        new Integer(10));
        
        loader.load(testDataFileN3, "http://test.example.org/test/load/file/native/rdf/base/uri");
        
        repositoryManager.shutDown();
        
        // NOTE: LoaderMain automatically shuts down the repository after a single call to load
        
        // Therefore, it is okay to create another repository manager here
        
        RepositoryManager repositoryManagerAfter = LoaderMain.getRepositoryManager(repositoryFolder, "native");
        
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
        RepositoryManager repositoryManagerBefore = LoaderMain.getRepositoryManager(repositoryFolder, "native");
        
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
        
        RepositoryManager repositoryManager = LoaderMain.getRepositoryManager(repositoryFolder, "native");
        
        LoaderMain loader =
                new LoaderMain(repositoryManager, new Integer(20),
                        new Integer(10));
        
        InputStream testResource1 = this.getClass().getResourceAsStream("loadermaintest-1.n3");
        
        loader.load(testResource1, RDFFormat.N3, "http://test.example.org/test/load/file/native/rdf/base/uri");
        
        repositoryManager.shutDown();
        
        // NOTE: LoaderMain automatically shuts down the repository after a single call to load
        
        // Therefore, it is okay to create another repository manager here
        
        RepositoryManager repositoryManagerAfter = LoaderMain.getRepositoryManager(repositoryFolder, "native");
        
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
}
