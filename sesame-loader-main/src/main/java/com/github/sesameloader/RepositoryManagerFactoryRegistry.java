/**
 * 
 */
package com.github.sesameloader;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates repository managers based on a string identifying the repository manager.
 * 
 * @author Peter Ansell p_ansell@yahoo.com
 */
public class RepositoryManagerFactoryRegistry
{
    private static final Logger LOG = LoggerFactory.getLogger(RepositoryManagerFactoryRegistry.class);

    private static volatile RepositoryManagerFactoryRegistry instance;
    
    private ConcurrentHashMap<String, RepositoryManagerFactory> services =
            new ConcurrentHashMap<String, RepositoryManagerFactory>();
    
    /**
     * 
     */
    public RepositoryManagerFactoryRegistry()
    {
        final ServiceLoader<RepositoryManagerFactory> serviceLoader =
                java.util.ServiceLoader.load(RepositoryManagerFactory.class,
                        RepositoryManagerFactory.class.getClassLoader());
        
        final Iterator<RepositoryManagerFactory> servicesIterator = serviceLoader.iterator();
        
        while(servicesIterator.hasNext())
        {
            RepositoryManagerFactory factory = servicesIterator.next();
            RepositoryManagerFactory putIfAbsent = services.putIfAbsent(factory.getKey(), factory);
            
            if(putIfAbsent != null)
            {
                LOG.error("RepositoryManagerFactory with key="+factory.getKey()+" class="+putIfAbsent.getClass().getName()+" replaced the factory="+factory.getClass().getName());
            }
        }
    }
    
    public Map<String, RepositoryManagerFactory> getAll()
    {
        return Collections.unmodifiableMap(services);
    }
    
    public RepositoryManagerFactory get(String key)
    {
        return services.get(key);
    }

    public static RepositoryManagerFactoryRegistry getInstance()
    {
        if(instance == null)
        {
            synchronized(RepositoryManagerFactoryRegistry.class)
            {
                if(instance == null)
                {
                    instance = new RepositoryManagerFactoryRegistry();
                }
            }
        }
        
        return instance;
    }
}
