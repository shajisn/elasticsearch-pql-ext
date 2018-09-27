package org.elasticsearch.pql.nlp;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.deeplearning4j.models.embeddings.loader.WordVectorSerializer;
import org.deeplearning4j.models.sequencevectors.SequenceVectors;
import org.deeplearning4j.models.sequencevectors.interfaces.SequenceElementFactory;
import org.deeplearning4j.models.sequencevectors.serialization.VocabWordFactory;
import org.deeplearning4j.models.word2vec.VocabWord;
import org.elasticsearch.SpecialPermission;

public class NLPSequenceSearcher {

	private static final Logger log = LogManager.getLogger(NLPSequenceSearcher.class);

	private static NLPSequenceSearcher instance;

	private InputStream inputStream = null;
	
	private SequenceVectors<VocabWord> vectors = null;

	private NLPSequenceSearcher() throws Exception {
		/*
		 * Load the model and test with some data
		 */
		log.info("Loading the NLP Model ...");
//		ClassLoader classloader = Thread.currentThread().getContextClassLoader();
//		inputStream = classloader.getResourceAsStream("nlp.model");
		
		try {
			SecurityManager sm = System.getSecurityManager();
			if (sm != null) {
				sm.checkPermission(new SpecialPermission());
			}
			    
//			File fModel = new File("/data/seq_vec.model");
			File fModel = AccessController.doPrivileged(
	            (PrivilegedExceptionAction<File>)
	                () -> new File("E:\\Program Files\\Elastic Search\\6.3.2\\plugins\\elasticsearch-pql\\data\\seq_vec.model")
	        );
			
			inputStream = new FileInputStream(fModel);
	    } catch (PrivilegedActionException e) {
	        // e.getException() should be an instance of IOException
	        // as only checked exceptions will be wrapped in a
	        // PrivilegedActionException.
	        throw (IOException) e.getException();
	    }
		
		log.info("NLP Model is loaded into memory ...");
		System.out.println("NLP Model is loaded into memory ...");
		
		SequenceElementFactory<VocabWord> f1 = new VocabWordFactory();
		vectors = WordVectorSerializer.readSequenceVectors(f1, inputStream);
		if (vectors == null) {
			log.error("NLPSequenceSearcher reading sequence vector from model failed.");
			System.out.println("NLPSequenceSearcher reading sequence vector from model failed.");
			throw new Exception("NLPSequenceSearcher reading sequence vector from model failed.");
		}
		else {
			log.info("Model Loading Successful");
			System.out.println("Model Loading Successful");
		}
	}

	@Override
	public void finalize() {
		log.info("NLPSequenceSearcher is getting destroyed ...");
		if (inputStream != null) {
			try {
				inputStream.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public static synchronized NLPSequenceSearcher getInstance() throws Exception {
		if (instance == null) {
			log.info("NLPSequenceSearcher creating instance ...");
			instance = new NLPSequenceSearcher();
		}
		return instance;
	}

	/**
	 * function to check word similarity
	 * 
	 * @param searchWord
	 * @return
	 * @throws Exception
	 */
	public Collection<String> findNearWords(String searchWord) throws Exception {

		try {
			log.info("NLPSequenceSearcher finding sequence vector for word [" + searchWord + "].");
			System.out.println("NLPSequenceSearcher finding sequence vector for word [" + searchWord + "].");
			if(vectors == null) {
				log.error("Loading NLP model is UnSuccesfull !!!");
				return null;
			}

			log.info("Finding Words near to " + searchWord);
			System.out.println("Finding Words near to " + searchWord);
			if (searchWord != null && searchWord.trim().length() > 0) {
				searchWord = searchWord.replaceAll(" ", "_");
				long stime = Calendar.getInstance().getTimeInMillis();

				// Prints out the closest 100 words
				Collection<String> lst = new ArrayList<String>();
				try {
					lst = vectors.wordsNearest(searchWord, 10);
				} catch (Exception e) {
					log.error("Could not find near by words. Error = " + e.getLocalizedMessage());
					e.printStackTrace();
				}
				long etime = Calendar.getInstance().getTimeInMillis();
				log.info("Look up Time is : " + (etime - stime) + " MilliSeconds");
				if (lst != null) {
					log.info("10 Words closest to '" + searchWord + "': " + Arrays.toString(lst.toArray()));
					System.out.println("10 Words closest to '" + searchWord + "': " + Arrays.toString(lst.toArray()));
				} else {
					log.info("Word Not matched in Corpus !!!!");
					System.out.println("Word Not matched in Corpus !!!!");
				}
				return lst;
			} else {
				log.info("No Words given to search !!!");
				return null;
			}
		} catch (Exception e) {
			throw e;
		} finally {

		}
	}
	
	public static void main(String[] args) throws Exception {
		System.out.println("Testing NLPSequenceSearcher ....");
		try {
			NLPSequenceSearcher thisIns = NLPSequenceSearcher.getInstance();
			thisIns.findNearWords("trump");
		} catch (Exception e) {
			System.out.println("Error ---------" + e.getMessage());
			e.printStackTrace();
		}
	}

}
