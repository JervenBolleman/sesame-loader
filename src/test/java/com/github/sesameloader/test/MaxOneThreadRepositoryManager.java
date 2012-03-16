/**
 * 
 */
package com.github.sesameloader.test;

import org.openrdf.model.ValueFactory;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;

import com.github.sesameloader.RepositoryManager;

/**
 * Dummy class used to test the maximum threads restriction.
 * 
 * @author Peter Ansell p_ansell@yahoo.com
 */
public class MaxOneThreadRepositoryManager implements RepositoryManager
{

    @Override
    public RepositoryConnection getConnection() throws RepositoryException
    {
        return null;
    }

    @Override
    public void shutDown() throws SailException, RepositoryException
    {
    }

    @Override
    public ValueFactory getValueFactory()
    {
        return null;
    }

    @Override
    public Integer getMaximumThreads()
    {
        return 1;
    }
    
}
