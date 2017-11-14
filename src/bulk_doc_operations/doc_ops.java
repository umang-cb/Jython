package bulk_doc_operations;

import java.util.List;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;

import rx.Observable;
import rx.functions.Func1;

public class doc_ops {

	public void bulkUpsert(Bucket bucket, List<JsonDocument> documents){
		Observable.from(documents)
			    .flatMap(new Func1<JsonDocument, Observable<JsonDocument>>() {
			        @Override
			        public Observable<JsonDocument> call(final JsonDocument docToInsert) {
			            return bucket.async().upsert(docToInsert);
			        }
			    })
			    .last()
			    .toBlocking()
			    .single();

	}
	
	public void bulkSet(Bucket bucket, List<JsonDocument> documents){
		Observable.from(documents)
			    .flatMap(new Func1<JsonDocument, Observable<JsonDocument>>() {
			        @Override
			        public Observable<JsonDocument> call(final JsonDocument docToInsert) {
			            return bucket.async().insert(docToInsert);
			        }
			    })
			    .last()
			    .toBlocking()
			    .single();

	}
	
	public List<JsonDocument> bulkGet(Bucket bucket, final List<String> ids) {
	    return Observable
	        .from(ids)
	        .flatMap(new Func1<String, Observable<JsonDocument>>() {
	            @Override
	            public Observable<JsonDocument> call(String id) {
	                return bucket.async().get(id);
	            }
	        })
	        .toList()
	        .toBlocking()
	        .single();
	}
	
}