/**
 * 
 */
package com.github.sesameloader.test;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.github.sesameloader.RepositoryManagerFactoryRegistry;

/**
 * @author Peter Ansell p_ansell@yahoo.com
 * 
 */
public class RepositoryManagerFactoryRegistryTest
{
    
    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
    }
    
    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
    }
    
    /**
     * Tests the default constructor to make sure that its behaviour matches .getInstance(),
     * although this method should not be used as it is slower than .getInstance().
     */
    @Test
    public void testRepositoryManagerFactoryRegistry()
    {
        RepositoryManagerFactoryRegistry registry = new RepositoryManagerFactoryRegistry();
        
        Assert.assertNotNull(registry.getAll());
        
        // Update the following if more RepositoryManagerFactory instances are added
        Assert.assertEquals(2, registry.getAll().size());
        
        Assert.assertNotNull(registry.get("owlim"));
        
        Assert.assertNotNull(registry.get("native"));
    }
    
    /**
     * Test method for
     * {@link com.github.sesameloader.RepositoryManagerFactoryRegistry#getInstance()}.
     * 
     * Tests that its behaviour is as expected and there are two factories available, and we can get
     * both owlim and native factories as expected.
     */
    @Test
    public void testGetInstance()
    {
        RepositoryManagerFactoryRegistry registry = RepositoryManagerFactoryRegistry.getInstance();
        
        Assert.assertNotNull(registry.getAll());
        
        // Update the following if more RepositoryManagerFactory instances are added
        Assert.assertEquals(2, registry.getAll().size());
        
        Assert.assertNotNull(registry.get("owlim"));
        
        Assert.assertNotNull(registry.get("native"));
    }
    
}
