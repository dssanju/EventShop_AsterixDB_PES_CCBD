package com.eventshop.eventshoplinux.service;


 
import static com.eventshop.eventshoplinux.constant.Constant.CONTEXT;
import static com.eventshop.eventshoplinux.constant.Constant.PATH_DS;
import static com.eventshop.eventshoplinux.constant.Constant.json;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.eventshop.eventshoplinux.DAO.datasource.DataSourceManagementDAO;
import com.eventshop.eventshoplinux.domain.datasource.DataSource;
import com.eventshop.eventshoplinux.domain.datasource.DataSourceListElement;
import com.eventshop.eventshoplinux.domain.login.User;
import com.eventshop.eventshoplinux.util.commonUtil.Config;
import com.eventshop.eventshoplinux.util.datasourceUtil.DataSourceParser;
import com.eventshop.eventshoplinux.domain.common.EmageElement;
import com.eventshop.eventshoplinux.domain.common.Result;
import com.google.gson.Gson;
import com.google.gson.JsonObject;



@Path("/datasourceservice")
public class DataSourceService {
	
	
	@PUT
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/datasources")
	public Result saveDataSource(DataSource source){
        DataSourceManagementDAO dao=new DataSourceManagementDAO();     
        Result result=dao.saveDataSource(source);
       // source.setSrcID(datasourceSrcId);
       // parsing causes us issues -- not needed as we are using servlet to run datasources -- sanjukta
        //DataSourceParser dsParser=new DataSourceParser();
        //dsParser.init();
       // dsParser.processData(source);       
        return result;

}

	
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/datasources/{id}")

	public DataSource getDataSource(@PathParam(value = "id") final String srcId){
		DataSourceManagementDAO dao=new DataSourceManagementDAO();
		int srcIdInt=new Integer(srcId).intValue();
		DataSource source=dao.getDataSource(srcIdInt);
		return source;
	} 
	
	
	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/datasources")
	
	/*
	 * This mathod returns datasource list for Admin/based on userid
	 */
	public DataSourceListElement[] getDataSourceList(User user){		
		DataSourceListElement[] datasourceTempArr=new DataSourceListElement[10];
		DataSourceListElement[] datasourceArr;
		DataSourceManagementDAO dao=new DataSourceManagementDAO();
		List<DataSourceListElement> listDatasource=dao.getDataSourceList(user);
		datasourceArr=listDatasource.toArray(datasourceTempArr);
		return datasourceArr;
	} 
	
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/getUserDatasourceList")
	
	/*
	 * This mathod returns datasource list for Admin/based on userid
	 */
	public DataSourceListElement[] getDataSourceList(@QueryParam("userId")int userId){
		User user = new User();
		user.setId(userId);		
		//DataSourceListElement[] datasourceTempArr=new DataSourceListElement[10];
		DataSourceListElement[] datasourceArr = null;
		DataSourceManagementDAO dao=new DataSourceManagementDAO();
		List<DataSourceListElement> listDatasource=dao.getDataSourceList(user);
		datasourceArr=listDatasource.toArray(new DataSourceListElement[listDatasource.size()]);
		return datasourceArr;
	} 
	
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/getdsemage")
	
	/*
	 * This method returns data source emage
	 */
	public Result getDataSourceEmage(@QueryParam("dsId")String dsId){
		
		DataSourceManagementDAO dao=new DataSourceManagementDAO();
		return dao.getDataSourceEmage(dsId);
		
	} 
	
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/dsemage/{id}")
	public EmageElement getDataSourceEmageElement(@PathParam(value="id") final String dsId){
		//DataSourceManagementDAO dao=new DataSourceManagementDAO();
		String vizFilePath = Config.getProperty(CONTEXT) + PATH_DS + dsId + json;
		System.out.println(vizFilePath);
		BufferedReader br;
		try {
			File vizFile = new File(vizFilePath);
			if(vizFile.exists()){
				br = new BufferedReader(new FileReader(vizFilePath));
				EmageElement eme = new Gson().fromJson(br, EmageElement.class);
				if(eme.row * eme.col > 150000)	// reduce emage size that return to the browser			eme.reduceSize(1);
				return eme;
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		return null;	
		
	} 
	
}
