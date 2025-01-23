package akhil.initialize;


import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import akhil.ApiCallProcessor;
import akhil.SQLProcessor;
import akhil.DataUnlimited.model.types.Types;

public class ContextListener implements ServletContextListener {
	
    public ContextListener() {
        
    }

    public void contextDestroyed(ServletContextEvent sce)  { 
         
    	//SQLite.driverDeregister();
    }

    public void contextInitialized(ServletContextEvent sce)  { 
    	
    	ServletContext context = sce.getServletContext();
    	SQLProcessor.initialize(context.getInitParameter("dbUrlWindows"), context.getInitParameter("dbUrlUnix"));
    	Types.setDBCLASS(context.getInitParameter("dbclass"));
    	Types.setDBURL(context.getInitParameter(SQLProcessor.getDBUrl()));
    	ApiCallProcessor.initialize();
    	ApiCallProcessor.setLogDir(context.getInitParameter("logdir"));
    	
    }
	
}
