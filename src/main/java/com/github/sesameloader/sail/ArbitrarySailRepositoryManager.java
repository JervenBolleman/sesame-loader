/**
 * 
 */
package com.github.sesameloader.sail;

import org.openrdf.model.ValueFactory;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.Sail;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.sail.SailException;

import com.github.sesameloader.RepositoryManager;

/**
 * @author Peter Ansell p_ansell@yahoo.com
 *
 */
public class ArbitrarySailRepositoryManager implements RepositoryManager
{
    
    private SailRepository upstreamRepository;

    /**
     * Constructs an ArbitrarySailRepositoryManager as a wrapper around any sail
     */
    public ArbitrarySailRepositoryManager(Sail upstreamSail)
    {
        this.upstreamRepository = new SailRepository(upstreamSail);
    }
    
    /* (non-Javadoc)
     * @see com.github.sesameloader.RepositoryManager#getConnection()
     */
    @Override
    public RepositoryConnection getConnection() throws RepositoryException
    {
        return this.upstreamRepository.getConnection();
    }
    
    /* (non-Javadoc)
     * @see com.github.sesameloader.RepositoryManager#shutDown()
     */
    @Override
    public void shutDown() throws SailException, RepositoryException
    {
        this.upstreamRepository.shutDown();
    }
    
    /* (non-Javadoc)
     * @see com.github.sesameloader.RepositoryManager#getValueFactory()
     */
    @Override
    public ValueFactory getValueFactory()
    {
        return this.upstreamRepository.getValueFactory();
    }
    
}
