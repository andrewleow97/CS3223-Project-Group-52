package simpledb.index.btree;

import simpledb.file.BlockId;
import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.query.Constant;

/**
 * An object that holds the contents of a B-tree leaf block.
 * @author Edward Sciore
 */
public class BTreeLeaf {
   private Transaction tx;
   private Layout layout;
   private Constant searchkey;
   private BTPage contents;
   private int currentslot;
   private String filename;
   private String opr;

   /**
    * Opens a buffer to hold the specified leaf block.
    * The buffer is positioned immediately before the first record
    * having the specified search key (if any).
    * @param blk a reference to the disk block
    * @param layout the metadata of the B-tree leaf file
    * @param searchkey the search key value
    * @param tx the calling transaction
    * @param opr operator for comparison
    */
   public BTreeLeaf(Transaction tx, BlockId blk, Layout layout, Constant searchkey, String opr) {
      this.tx = tx;
      this.layout = layout;
      this.searchkey = searchkey;
      contents = new BTPage(tx, blk, layout);
      currentslot = contents.findSlotBefore(searchkey);
      filename = blk.fileName();     
      this.opr = opr;
   }

   /**
    * Closes the leaf page.
    */
   public void close() {
      contents.close();
   }

   /**
    * Moves to the next leaf record that satisfies the condition 
    * of the operator and search key.
    * Returns false if there is no more such records.
    * @return false if there are no more leaf records for the search key
    */
   public boolean next() {
      currentslot++;
      if (currentslot >= contents.getNumRecs()) 
         return tryOverflow();
      else if (isValid(contents.getDataVal(currentslot), searchkey)) { 
    	  // iterate to next 
    	  while (isValid(contents.getDataVal(currentslot), searchkey) && currentslot < contents.getNumRecs()) {
    		  if (isSatisfied(contents.getDataVal(currentslot), searchkey))
    			  return true;
    		  currentslot++;
    	  }
    	  return tryOverflow();
      }
      else 
         return tryOverflow();
   }
   

   /**
    * Sets currentslot to search for the first block
    */
   public void pushFirst() {
	   currentslot = -1;
   }
   
   /**
    * Checks if record pointed to by currentslot is valid to iterate 
    * over based on the operator and search key. 
    * Ensures that only valid records are iterated over such that a range of records
    * can be obtained if operator is not "=".
    * Returns false if the record is not valid.
    * @return false if there are no more valid leaf records 
    */
   private boolean isValid(Constant lhs, Constant rhs) {
	   switch(this.opr) {
	      case "=":
	    	  return lhs.compareTo(rhs) == 0; // tuples = are valid
	      case "<":
	    	  return lhs.compareTo(rhs) < 0; // tuples < are valid
	      case "<=":
	    	  return lhs.compareTo(rhs) < 0 || lhs.compareTo(rhs) == 0; // tuples <= are valid
	      case ">":
	    	  return lhs.compareTo(rhs) >= 0; // tuples >= are valid
	      case ">=":
	    	  return lhs.compareTo(rhs) > 0 || lhs.compareTo(rhs) == 0; //tuples >= are valid
	      case "!=":
	    	  return true; // search all
	      case "<>":
	    	  return true; // search all
		  default:
			  return false;  
	      }
   }
   
   /**
    * Checks if record pointed to by currentslot satisfies the operator and search key.
    * Returns true if LHS opr RHS is true.
    * Returns false if the above condition is not satisfied.
    * @return false if there are no more valid leaf records 
    */
   private boolean isSatisfied(Constant lhs, Constant rhs) {
	   switch(this.opr) {
	      case "=":
	    	  return lhs.compareTo(rhs) == 0;
	      case "<":
	    	  return lhs.compareTo(rhs) < 0;
	      case "<=":
	    	  return lhs.compareTo(rhs) < 0 || lhs.compareTo(rhs) == 0;
	      case ">":
	    	  return lhs.compareTo(rhs) > 0;
	      case ">=":
	    	  return lhs.compareTo(rhs) > 0 || lhs.compareTo(rhs) == 0;
	      case "!=":
	    	  return lhs.compareTo(rhs) != 0;
	      case "<>":
	    	  return lhs.compareTo(rhs) != 0;
		  default:
			  return false;  
	      }
   }

   /**
    * Returns the dataRID value of the current leaf record.
    * @return the dataRID of the current record
    */
   public RID getDataRid() {
      return contents.getDataRid(currentslot);
   }

   /**
    * Deletes the leaf record having the specified dataRID
    * @param datarid the dataRId whose record is to be deleted
    */
   public void delete(RID datarid) {
      while(next())
         if(getDataRid().equals(datarid)) {
            contents.delete(currentslot);
            return;
         }
   }

   /**
    * Inserts a new leaf record having the specified dataRID
    * and the previously-specified search key.
    * If the record does not fit in the page, then 
    * the page splits and the method returns the
    * directory entry for the new page;
    * otherwise, the method returns null.  
    * If all of the records in the page have the same dataval,
    * then the block does not split; instead, all but one of the
    * records are placed into an overflow block.
    * @param datarid the dataRID value of the new record
    * @return the directory entry of the newly-split page, if one exists.
    */
   public DirEntry insert(RID datarid) {
      if (contents.getFlag() >= 0 && contents.getDataVal(0).compareTo(searchkey) > 0) {
         Constant firstval = contents.getDataVal(0);
         BlockId newblk = contents.split(0, contents.getFlag());
         currentslot = 0;
         contents.setFlag(-1);
         contents.insertLeaf(currentslot, searchkey, datarid); 
         return new DirEntry(firstval, newblk.number());  
      }

      currentslot++;
      contents.insertLeaf(currentslot, searchkey, datarid);
      if (!contents.isFull())
         return null;
      // else page is full, so split it
      Constant firstkey = contents.getDataVal(0);
      Constant lastkey  = contents.getDataVal(contents.getNumRecs()-1);
      if (lastkey.equals(firstkey)) {
         // create an overflow block to hold all but the first record
         BlockId newblk = contents.split(1, contents.getFlag());
         contents.setFlag(newblk.number());
         return null;
      }
      else {
         int splitpos = contents.getNumRecs() / 2;
         Constant splitkey = contents.getDataVal(splitpos);
         if (splitkey.equals(firstkey)) {
            // move right, looking for the next key
            while (contents.getDataVal(splitpos).equals(splitkey))
               splitpos++;
            splitkey = contents.getDataVal(splitpos);
         }
         else {
            // move left, looking for first entry having that key
            while (contents.getDataVal(splitpos-1).equals(splitkey))
               splitpos--;
         }
         BlockId newblk = contents.split(splitpos, -1);
         return new DirEntry(splitkey, newblk.number());
      }
   }

   /**
    * Checks for overflow chains.
    * Returns true if new block is obtained and valid.
    * Returns false if no more valid records.
    * @return false if there are no more valid leaf records 
    */
   private boolean tryOverflow() {
      Constant firstkey = contents.getDataVal(0);
      int flag = contents.getFlag();
      if (!isValid(firstkey, searchkey) || flag < 0) 
         return false;
      contents.close();
      BlockId nextblk = new BlockId(filename, flag);
      contents = new BTPage(tx, nextblk, layout);
      currentslot = 0;
      return true;
   }
}
