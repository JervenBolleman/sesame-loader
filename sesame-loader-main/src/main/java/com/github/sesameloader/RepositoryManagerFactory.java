/**
 * 
 */
package com.github.sesameloader;

import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;

/**
 * @author Peter Ansell p_ansell@yahoo.com
 * 
 */
public interface RepositoryManagerFactory
{
    /**
     * Creates a repository manager using the given dataDirectory location for storage.
     * 
     * @param dataDirectory
     *            The location to use for data storage.
     * @return The repository manager the encapsulates access to the underlying repository.
     * @throws SailException If an underlying Sail failed to be initialised for any reason.
     * @throws RepositoryException If an underlying Repository failed to be initialised for any reason.
     */
    RepositoryManager createRepositoryManager(String dataDirectory) throws RepositoryException, SailException;
    
    /**
     * 
     * @return A string used to identify this factory as the correct factory. This must be unique
     *         across all factories or the behaviour will be inconsistent.
     */
    String getKey();
    
}
