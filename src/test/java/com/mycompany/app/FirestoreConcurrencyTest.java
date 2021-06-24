package com.mycompany.app;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.firestore.*;
import edu.umd.cs.mtc.MultithreadedTestCase;
import edu.umd.cs.mtc.TestFramework;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class FirestoreConcurrencyTest extends MultithreadedTestCase {

    public static final String COLLECTION_NAME = "entities";
    public static final String FIELD_VALUE = "dsadssaasaasdsasadsadsadassadasdasdsa";
    private Logger log = LoggerFactory.getLogger(FirestoreConcurrencyTest.class);
    private Credentials credentials;
    private String keyFilePath = "";
    private String projectId = "";

    public FirestoreConcurrencyTest() throws Exception {
        credentials = GoogleCredentials.fromStream(new FileInputStream(keyFilePath))
                .createScoped("https://www.googleapis.com/auth/cloud-platform");

        log.info("Deleting documents before test");
        try (Firestore firestore = createFirestore()) {
            firestore.recursiveDelete(getCollection(firestore)).get();
        }
    }

    public void thread1() throws Exception {
        work();
    }

    public void thread2() throws Exception {
        work();
    }

    public void thread3() throws Exception {
        work();
    }

    public void thread4() throws Exception {
        work();
    }


    private void work() throws Exception {

        log.info("Create firestore");

        try (Firestore db = createFirestore()) {
            log.info("Trying to create document if not exists");
            final Callable<DocumentReference> documentReferenceSupplier = () -> createDocumentIfNotExists(db);
            final DocumentReference documentReference = doWithRetry(documentReferenceSupplier);

            log.info("Created document {}",documentReference);

            log.info("Trying to delete document");
            DocumentReference finalDocumentReference = documentReference;
            Callable callable = () -> db.runTransaction(t -> {
                t.delete(finalDocumentReference);
                return documentReference;
            }).get();
            doWithRetry(callable);
            log.info("Deleted document");
        }
    }

    private DocumentReference doWithRetry(final Callable<DocumentReference> callable) throws Exception {
        DocumentReference result;
        while (true) {
            try {
                result = callable.call();
                if (result != null) return result;
                Thread.sleep(30);
                log.info("Trying again");
            } catch (ExecutionException ex) {
                Throwable cause = ex.getCause();
                if (cause != null) {
                    while ((cause = cause.getCause()) != null && !(cause instanceof ApiException)) {
                    }
                }
                log.info("Caught exception ", cause);
                if (cause != null) {
                    log.info("Exception is retryable {}", ((ApiException) cause).isRetryable());
                }
                if (cause != null && (((ApiException) cause).isRetryable() || ((ApiException) cause).getStatusCode().getCode() == StatusCode.Code.ABORTED ||
                        ((ApiException) cause).getStatusCode().getCode() == StatusCode.Code.INVALID_ARGUMENT) || ex.getMessage().contains("transaction closed")) {
                    log.debug("Caught retryable exception", ex);
                    Thread.sleep(50);
                } else {
                    log.error("Caught non-retryable exception", ex);
                    throw ex;
                }
            } catch (TimeoutException e) {
                log.info("Timeout exception");
                Thread.sleep(500);
            }
            log.info("Trying again to create document if not exists");
        }
    }

    private DocumentReference createDocumentIfNotExists(final Firestore db) throws InterruptedException, ExecutionException, TimeoutException {
        final DocumentReference documentReference = db.runTransaction(t -> {
            final Query query = getCollection(db).whereEqualTo("field1", FIELD_VALUE);
            final boolean empty = t.get(query).get().isEmpty();
            if (empty) {
                final HashMap<String, Object> map = new HashMap<>();
                for (int i = 0; i < 10; i++) {
                    map.put("field" + i, FIELD_VALUE);
                }

                final DocumentReference documentRef = getCollection(db).document(UUID.randomUUID().toString());
                t.set(documentRef, map);
                return documentRef;
            }
            return null;
        }).get(10, TimeUnit.SECONDS);
        return documentReference;
    }

    private CollectionReference getCollection(final Firestore db) {
        return db.collection(COLLECTION_NAME);
    }

    private Firestore createFirestore() {
        return FirestoreOptions.getDefaultInstance().toBuilder()
                .setProjectId(projectId)
                .setChannelProvider(InstantiatingGrpcChannelProvider.newBuilder().build())
                .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
                .build().getService();
    }

    @Test
    public void testConcurrency() throws Throwable {
        TestFramework.runManyTimes(new FirestoreConcurrencyTest(), 100, 10, 300);
    }
}
