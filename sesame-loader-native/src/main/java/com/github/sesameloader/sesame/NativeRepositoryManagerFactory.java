/**
 * 
 */
package com.github.sesameloader.sesame;

import java.io.File;

import org.kohsuke.MetaInfServices;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;

import com.github.sesameloader.RepositoryManager;
import com.github.sesameloader.RepositoryManagerFactory;

/**
 * Creates Native Repository Managers as needed.
 * 
 * @author Peter Ansell p_ansell@yahoo.com
 */
@MetaInfServices()
public class NativeRepositoryManagerFactory implements RepositoryManagerFactory
{
    private static final String KEY = "native";
    
    /**
     * 
     */
    public NativeRepositoryManagerFactory()
    {
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.github.sesameloader.RepositoryManagerFactory#createRepositoryManager(java.lang.String)
     */
    @Override
    public RepositoryManager createRepositoryManager(String dataDirectory) throws RepositoryException, SailException
    {
        return new NativeRepositoryManager(new File(dataDirectory));
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.github.sesameloader.RepositoryManagerFactory#getKey()
     */
    @Override
    public String getKey()
    {
        return KEY;
    }
    
}
