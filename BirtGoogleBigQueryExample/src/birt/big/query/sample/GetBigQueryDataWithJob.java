package birt.big.query.sample;
import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Date;
import java.util.List;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson.JacksonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.Bigquery.Jobs.Insert;
import com.google.api.services.bigquery.model.GetQueryResultsResponse;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationExtract;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.QueryRequest;
import com.google.api.services.bigquery.model.QueryResponse;
import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableRow;


public class GetBigQueryDataWithJob {

	static String privateKey = "D:/gurkan/9cbcd8ef83f742c51b5644129fb03856/GoogleBigQuery/BirtGoogleBigQueryExample/6671d8c740a6ac373a156a648a253fa8e857a932-privatekey.p12";
	
	private static final String SCOPE = "https://www.googleapis.com/auth/bigquery";
	private static final HttpTransport TRANSPORT = new NetHttpTransport();
	private static final JsonFactory JSON_FACTORY = new JacksonFactory();
	private static Bigquery bigquery;
	//private static String QUERY = "SELECT year, SUM(record_weight) as births FROM publicdata:samples.natality GROUP BY year";
	private static String QUERY = "SELECT sum( RN ),sum( S_RUR ) FROM dataset.gurkandata group by SH_NN, NYEARISO, W, model";
	String QUERY2 = "SELECT TOP(word, 50), COUNT(*) FROM publicdata:samples.shakespeare";

	
	public static String PROJECT_ID="nth-suprstate-560";
	
	public static Bigquery setupConnection() throws GeneralSecurityException, IOException{

		GoogleCredential credential = new GoogleCredential.Builder().setTransport(TRANSPORT)
				.setJsonFactory(JSON_FACTORY)
				.setServiceAccountId("160087228600-i8dk6g3es9j1k3lbljb3dib6se09fim5@developer.gserviceaccount.com")
				.setServiceAccountScopes(SCOPE)
				.setServiceAccountPrivateKeyFromP12File(new File(privateKey)).build();
		
		

		bigquery = new Bigquery.Builder(TRANSPORT, JSON_FACTORY, credential).setApplicationName("nth-suprstate-560").setHttpRequestInitializer(credential).build();
		System.out.println("connectionOK");
		return bigquery;
	}
	public List<TableRow> executeQuery( String query) throws IOException{


		QueryRequest queryInfo = new QueryRequest().setQuery(query);

		Bigquery.Jobs.Query queryRequest = bigquery.jobs().query("nth-suprstate-560", queryInfo);
		QueryResponse queryResponse = queryRequest.execute();
		return queryResponse.getRows();
	}

	public static JobReference startQuery(Bigquery bigquery, String projectId,String querySql) throws IOException {
		System.out.format("\nInserting Query Job: %s\n", querySql);

		Job job = new Job();
		JobConfiguration config = new JobConfiguration();
		JobConfigurationQuery queryConfig = new JobConfigurationQuery();
		config.setQuery(queryConfig);

		job.setConfiguration(config);
		
		JobConfigurationExtract extract = new JobConfigurationExtract();
		//extract.set
		extract.setDestinationFormat("NEWLINE_DELIMITED_JSON");
		extract.setDestinationUri("gs://gurkandata/test.json");
		//config.setExtract(extract);
		
		queryConfig.setQuery(querySql);
		Insert insert = bigquery.jobs().insert(projectId, job);
		insert.setProjectId(projectId);
		JobReference jobId = insert.execute().getJobReference();

		System.out.format("\nJob ID of Query Job is: %s\n", jobId.getJobId());

		return jobId;
	}
	
	private static Job checkQueryResults(Bigquery bigquery, String projectId, JobReference jobId) throws IOException, InterruptedException {
		    // Variables to keep track of total query time
		    long startTime = System.currentTimeMillis();
		    long elapsedTime;

		    while (true) {
		      Job pollJob = bigquery.jobs().get(projectId, jobId.getJobId()).execute();
		      elapsedTime = System.currentTimeMillis() - startTime;
		      System.out.format("Job status (%dms) %s: %s\n", elapsedTime, jobId.getJobId(), pollJob.getStatus().getState());
		      if (pollJob.getStatus().getState().equals("DONE")) {
		        
		    	  return pollJob;
		      }
		      // Pause execution for one second before polling job status again, to
		      // reduce unnecessary calls to the BigQUery API and lower overall
		      // application bandwidth.
		      Thread.sleep(1000);
		    }
		  }
	
	private static void displayQueryResults(Bigquery bigquery, String projectId, Job completedJob) throws IOException {
		GetQueryResultsResponse queryResult = bigquery
											.jobs()
											.getQueryResults(projectId,completedJob.getJobReference().getJobId())
											.execute();
		
		List<TableRow> rows = queryResult.getRows();
		
		System.out.print("\nQuery Results:\n------------\n");
		for (TableRow row : rows) {
			for (TableCell field : row.getF()) {
				System.out.printf("%-50s", field.getV());
			}
			System.out.println();
		}
	}
	
	public static void main(String[] args) throws Exception, GeneralSecurityException {
//		GETBIGQUERYDATAWITHJOB BQ = NEW GETBIGQUERYDATAWITHJOB();
//		BQ.SETUPCONNECTION();
//		SYSTEM.OUT.PRINTLN("BASLADI "+ NEW DATE());
		
		 Bigquery bigquery = setupConnection();
		
		// Start a Query Job
	    //String querySql = "SELECT TOP(word, 50), COUNT(*) FROM publicdata:samples.shakespeare";
	    String querySql = "SELECT *  FROM publicdata:samples.shakespeare";
	    
	    JobReference jobId = startQuery(bigquery, PROJECT_ID, querySql);

	    // Poll for Query Results, return result output
	    Job completedJob = checkQueryResults(bigquery, PROJECT_ID, jobId);

	    // Return and display the results of the Query Job
	    displayQueryResults(bigquery, PROJECT_ID, completedJob);
	    
	    System.out.println("bitti");
	}
}
