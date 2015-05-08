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
import com.google.api.services.bigquery.model.QueryRequest;
import com.google.api.services.bigquery.model.QueryResponse;
import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableRow;


public class GetBigQueryData {

	String privateKey = "D:/gurkan/9cbcd8ef83f742c51b5644129fb03856/GoogleBigQuery/BirtGoogleBigQueryExample/6671d8c740a6ac373a156a648a253fa8e857a932-privatekey.p12";
	//String privateKey="/mnt/ebs1/privatekey.p12";
	
	private static final String SCOPE = "https://www.googleapis.com/auth/bigquery";
	private static final HttpTransport TRANSPORT = new NetHttpTransport();
	private static final JsonFactory JSON_FACTORY = new JacksonFactory();
	private Bigquery bigquery;
	//private static String QUERY = "SELECT year, SUM(record_weight) as births FROM publicdata:samples.natality GROUP BY year";
	private static String QUERY = "SELECT sum( RN ),sum( S_RUR ) FROM dataset.gurkandata group by SH_NN, NYEARISO, W, model";
	String QUERY2 = "SELECT TOP(word, 50), COUNT(*) FROM publicdata:samples.shakespeare";

	public void setupConnection() throws GeneralSecurityException, IOException{

		GoogleCredential credential = new GoogleCredential.Builder().setTransport(TRANSPORT)
				.setJsonFactory(JSON_FACTORY)
				.setServiceAccountId("160087228600-i8dk6g3es9j1k3lbljb3dib6se09fim5@developer.gserviceaccount.com")
				.setServiceAccountScopes(SCOPE)
				.setServiceAccountPrivateKeyFromP12File(new File(privateKey)).build();
		
		

		bigquery = new Bigquery.Builder(TRANSPORT, JSON_FACTORY, credential).setApplicationName("nth-suprstate-560").setHttpRequestInitializer(credential).build();
		System.out.println("connectionOK");
	}
	public List<TableRow> executeQuery( String query) throws IOException{


		QueryRequest queryInfo = new QueryRequest().setQuery(query);

		Bigquery.Jobs.Query queryRequest = bigquery.jobs().query("nth-suprstate-560", queryInfo);
		QueryResponse queryResponse = queryRequest.execute();
		return queryResponse.getRows();
	}

	public static void main(String[] args) throws IOException, GeneralSecurityException {
		GetBigQueryData bq = new GetBigQueryData();
		bq.setupConnection();
		System.out.println("basladi "+ new Date());
		List<TableRow> rows = bq.executeQuery(QUERY);

		if( rows != null ){
			for (TableRow row : rows) {
			      for (TableCell field: row.getF()) {
			       System.out.printf("%s--", field.getV());
			      }
			}
		}
		System.out.println("bitti "+ new Date());
	}
}
