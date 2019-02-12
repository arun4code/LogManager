package de.tuberlin.dima.dbt.exercises.logging;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LogManager {

    /**
     * Buffer Manager dependency.
     * <p>
     * The Log Manager needs to interact with the Buffer Manager during
     * checkpointing and recovery.
     */
    protected BufferManager bufferManager;

    /**
     * The current log sequence number.
     */
    protected int currentLsn = 0;

    /**
     * The list of log records.
     */
    protected List<LogRecord> logRecords;

    /**
     * The active transaction table.
     * <p>
     * The transaction table maps transaction IDs to log sequence numbers.
     */
    protected Map<Integer, Integer> transactions;

    /**
     * The dirty page table.
     * <p>
     * The dirty page table maps page IDs to log sequence numbers.
     */
    protected Map<Integer, Integer> dirtyPages;

    /////
    ///// Functions that append log records
    /////

    /**
     * Add a BEGIN_OF_TRANSACTION entry to the log.
     *
     * @param transactionId The transaction ID.
     * @throws A LogManagerException if a transaction with the ID already exists.
     */
    public void beginTransaction(int transactionId) {        
    	if(this.transactions.containsKey(transactionId)) {
    		throw new LogManagerException("transaction ID already exists");
    	}
    	if(this.logRecords == null || this.logRecords.isEmpty()) {
    		this.logRecords = new ArrayList<>();
        		
    	}
    	
    	LogRecord lr = new LogRecord(currentLsn, transactionId);
    	currentLsn = currentLsn + 1;
    			
    	this.logRecords.add(lr);
    	this.transactions.put(transactionId, transactionId);
    	
    	
    }

    /**
     * Add an UPDATE entry to the log.
     *
     * @param transactionId The transaction ID.
     * @param pageId        The page ID.
     * @param elementId     The element ID.
     * @param oldValue      The old value.
     * @param newValue      The new value.
     * @throws A LogManagerException if the transaction with the ID does not exist.
     */
    public void update(int transactionId, int pageId, int elementId,
                       String oldValue, String newValue) {        
    	if(!this.transactions.containsKey(transactionId)) {
    		throw new LogManagerException("transaction ID doesn't already exists");
    	}
    	
    	List<LogRecord> tempList = new ArrayList<>();
    	
    	if(this.transactions.get(transactionId) != null) {
    		for(LogRecord lr : logRecords) {
    			if(lr.getTransactionId().equals(transactionId) &&
    					!lr.getType().equals(LogRecordType.UPDATE)) {
    				lr.setPreviousSequenceNumber(lr.getLogSequenceNumber());
    				
    				lr.setLogSequenceNumber(currentLsn);
    				
    				lr.setOldValue(oldValue);
    				lr.setNewValue(newValue);
    				lr.setElementId(elementId);
    				lr.setPageId(pageId);
    				lr.setType(LogRecordType.UPDATE);

    				currentLsn = currentLsn + 1;    				
    				tempList.add(lr);    				
    			}
    			else if(lr.getTransactionId().equals(transactionId) &&
		    					lr.getType().equals(LogRecordType.UPDATE)) {
    				tempList.add(lr);
    				
		    		LogRecord lrecord = new LogRecord(currentLsn, transactionId, lr.getLogSequenceNumber(),    						
							pageId, elementId, oldValue, newValue);
					
		    		tempList.add(lrecord);
					currentLsn = currentLsn + 1;
					
		    	}
	    	}
    	}
		this.logRecords = tempList;
    }

    /**
     * Add a COMMIT entry to the log.
     *
     * @param transactionId The transaction ID.
     * @throws A LogManagerException if the transaction with the ID does not exist.
     */
    public void commit(int transactionId) {    	
    	if(!this.transactions.containsKey(transactionId)) {
    		throw new LogManagerException("transaction ID doesn't already exists");
    	}
    	List<LogRecord> tempList = new ArrayList<>();
    	
    	if(this.transactions.get(transactionId) != null) {
    		for(LogRecord lr : logRecords) {
    			if(lr.getTransactionId().equals(transactionId) &&
    					!lr.getType().equals(LogRecordType.COMMIT)) {

    				lr.setPreviousSequenceNumber(lr.getLogSequenceNumber());
    				
    				lr.setLogSequenceNumber(currentLsn);
    				
    				lr.setType(LogRecordType.COMMIT);

    				currentLsn = currentLsn + 1;    				
    				tempList.add(lr);    				
    			}
    			else if(lr.getTransactionId().equals(transactionId) &&
    					lr.getType().equals(LogRecordType.COMMIT)) {
					tempList.add(lr);
					
		    		LogRecord lrecord = new LogRecord(currentLsn, LogRecordType.COMMIT, transactionId, lr.getLogSequenceNumber());
					
		    		tempList.add(lrecord);
					currentLsn = currentLsn + 1;					
		    	}
    		}
    	}
    	
    }

    /**
     * Add a END_OF_TRANSACTION entry to the log.
     *
     * @param transactionId The transaction ID.
     * @throws A LogManagerException if the transaction with the ID does not exist.
     */
    public void endTransaction(int transactionId) {
    	if(!this.transactions.containsKey(transactionId)) {
    		throw new LogManagerException("transaction ID doesn't already exists");
    	}
    	
    	List<LogRecord> tempList = new ArrayList<>();
    	
    	if(this.transactions.get(transactionId) != null) {
    		for(LogRecord lr : logRecords) {
    			if(lr.getTransactionId().equals(transactionId) &&
    					!lr.getType().equals(LogRecordType.END_OF_TRANSACTION)) {

    				lr.setPreviousSequenceNumber(lr.getLogSequenceNumber());
    				
    				lr.setLogSequenceNumber(currentLsn);
    				
    				lr.setType(LogRecordType.END_OF_TRANSACTION);

    				currentLsn = currentLsn + 1;    				
    				tempList.add(lr);    				
    			}    			
    		}
    	}
    }

    /////
    ///// Functions that implement checkpointing
    /////

    /**
     * Add a BEGIN_OF_CHECKPOINT entry to the log.
     */
    public void beginCheckpoint() {    	
		for(LogRecord lr : logRecords) {
			if(!lr.getType().equals(LogRecordType.BEGIN_OF_CHECKPOINT)) {				
				
				lr.setLogSequenceNumber(currentLsn);
				
				lr.setType(LogRecordType.BEGIN_OF_CHECKPOINT);
				
				lr.setTransactionId(null);		//TODO

				currentLsn = currentLsn + 1;
			}    			
		}    	
    }

    /**
     * Add a END_OF_CHECKPOINT entry to the log and write checkpointing data to disk.
     *
     * Should write:
     * <ul>
     *     <li>The LSN of the BEGIN_OF_CHECKPOINT entry.</li>
     *     <li>The transaction table.</li>
     *     <li>The dirty page table.</li>
     * </ul>
     */
    public void endCheckpoint() {

		for(LogRecord lr : logRecords) {
			if(!lr.getType().equals(LogRecordType.END_OF_CHECKPOINT)) {				
				int temp = lr.getTransactionId() == null ? 0 : lr.getTransactionId();
				lr.setLogSequenceNumber(currentLsn);
				
				lr.setType(LogRecordType.END_OF_CHECKPOINT);
				
				lr.setTransactionId(null);		//TODO

				currentLsn = currentLsn + 1;

				//setBufferManager(this);
				this.writeBeginningOfCheckpoint(temp);
				/*
				bufferManager = new BufferManager() {

					@Override
					public void writeElement(int pageId, int elementId, String value) {
						// TODO Auto-generated method stub
						
					}

					@Override
					public void writeBeginningOfCheckpoint(int lsn) {
						System.out.println(lsn);
						
					}

					@Override
					public void writeTransactionTable(Map<Integer, Integer> transactionTable) {
						// TODO Auto-generated method stub
						
					}

					@Override
					public void writeDirtyPageTable(Map<Integer, Integer> dirtyPageTable) {
						// TODO Auto-generated method stub
						
					}

					@Override
					public int getPageLsn(int pageId) {
						// TODO Auto-generated method stub
						return 0;
					}
					
				};
				*/
				//this.bufferManager.writeBeginningOfCheckpoint(temp);
			}    			
		}
    }

    public void setBufferManager(BufferManager bufferManager){
        this.bufferManager = bufferManager;
    }
    
    public void writeBeginningOfCheckpoint(int lsn) {		
		bufferManager.writeBeginningOfCheckpoint(lsn);
	}
    
    class MyBufferManager implements BufferManager {

		@Override
		public void writeElement(int pageId, int elementId, String value) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void writeBeginningOfCheckpoint(int lsn) {
			System.out.println(lsn);
		}

		@Override
		public void writeTransactionTable(Map<Integer, Integer> transactionTable) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void writeDirtyPageTable(Map<Integer, Integer> dirtyPageTable) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public int getPageLsn(int pageId) {
			// TODO Auto-generated method stub
			return 0;
		}
    	
    }
    
    /////
    ///// Functions that implement recovery
    /////

    /**
     * Analyse an undo/redo log starting from a specific LSN and update the
     * transaction and dirty page tables.
     *
     * The transaction and dirty page tables, which are passed to this function,
     * should be updated in-place.
     *
     * @param logRecords        The complete undo/redo log.
     * @param checkPointedLsn   The LSN at which the analysis path should start.
     * @param transactions      The transaction table.
     * @param dirtyPages        The dirty page table.
     */
    public void analysisPass(List<LogRecord> logRecords, int checkPointedLsn,
                             Map<Integer, Integer> transactions,
                             Map<Integer, Integer> dirtyPages) {
        // TODO Implement analysis pass
    }

    /**
     * Perform the redo pass for the given log records.
     *
     * An UPDATE statement that has to be redone should call
     * bufferManager.writeElement with the required page ID, element ID, and
     * the value that should be writen.
     *
     * @param logRecords    The complete undo/redo log.
     * @param dirtyPages    The dirty page table constructed during the analysis
     *                      path.
     */
    public void redoPass(List<LogRecord> logRecords,
                         Map<Integer, Integer> dirtyPages) {
        // TODO Implement undo pass
    }

    /**
     * Perform the undo pass for the given log entries.
     *
     * An UPDATE statement that has to be undone should call
     * bufferManager.writeElement with the required page ID, element ID, and
     * the value that should be writen.
     *
     * @param logRecords    The complete undo/redo log.
     * @param transactions  The transaction table constructed during the
     *                      analysis path.
     */
    public void undoPass(List<LogRecord> logRecords,
                         Map<Integer, Integer> transactions) {
        // TODO Implement redo pass
    }

    /////
    ///// Constructor / Getter / Setter (DO NOT CHANGE THIS CODE)
    /////

    /**
     * Constructs an empty undo/redo log.
     *
     * DO NOT CHANGE THIS CODE.
     *
     * @param bufferManager Dependency on a Buffer Manager.
     */
    public LogManager(BufferManager bufferManager) {
        this(bufferManager, 0, new ArrayList<>(), new HashMap<>(),
             new HashMap<>());
    }

    /**
     * Constructs an undo/redo log based on existing log records and transaction
     * and dirty page table.
     *
     * DO NOT CHANGE THIS CODE.
     *
     * @param bufferManager Dependency on a Buffer Manager.
     * @param currentLsn    The current LSN. New entries will be appended to the
     *                      log starting with this LSN.
     * @param logRecords    A list of existing log records.
     * @param transactions  A transaction table.
     * @param dirtyPages    A dirty page table.
     */
    public LogManager(BufferManager bufferManager, int currentLsn,
                      List<LogRecord> logRecords,
                      Map<Integer, Integer> transactions,
                      Map<Integer, Integer> dirtyPages) {
        this.bufferManager = bufferManager;
        this.currentLsn = currentLsn;
        this.logRecords = logRecords;
        this.transactions = transactions;
        this.dirtyPages = dirtyPages;
    }

    public List<LogRecord> getLogRecords() {
        return logRecords;
    }

}
