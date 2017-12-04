package AsycQuery;

import java.util.List;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.analytics.AsyncAnalyticsQueryRow;
import com.couchbase.client.java.analytics.SimpleAnalyticsQuery;
import rx.Observable;
import java.util.logging.Logger;

public class query {
	private final static Logger LOGGER = Logger.getLogger(query.class.getName());
	
	public List<String> run_query(Bucket bucket, SimpleAnalyticsQuery query){
		List<String> list = bucket.async().query(query)
				.flatMap(result -> result.rows()
						.mergeWith(result.errors()
								.flatMap(error -> Observable.<AsyncAnalyticsQueryRow>error(new RuntimeException("Analytics Query error: " + error.toString()))))
						.flatMap(row -> 
						Observable.just(row) // Wrap each row in it's own Observable 
						.subscribeOn(bucket.environment().scheduler()) // observe on a thread pool scheduler
						.flatMap(r -> {
							LOGGER.info("Working on " + r.value().get("id")); // Log shows different threads. 
							return r.value().containsKey("name") ? Observable.just(r.value().getString("name")) : Observable.<String>empty();
						})
								).toList()
						).toBlocking().single();
		return list;
	}
}
