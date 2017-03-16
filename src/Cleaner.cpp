/*
 * Copyright (C) 2017 Hops.io
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */

/*
 * File:   Cleaner.cpp
 * Author: Fabio Buso <buso@kth.se>
 *
 */

 #include "Cleaner.h"

 using namespace Utils;
 using namespace Utils::NdbC;

bool Cleaner::check(NdbRecAttr* preValue[], int column, const char* table){

  // Check that there are no more StorageDescriptors referencing the ColumnDescriptor_ID
  const NdbDictionary::Dictionary* pDatabase = getDatabase(mNdbConnection);
  const NdbDictionary::Table* pTable = getTable(pDatabase, table);

  // Define transaction
  NdbTransaction* pTransaction = startNdbTransaction(mNdbConnection);

  // Get scan operation
  NdbScanOperation* pScan_op = getNdbScanOperation(pTransaction, pTable);

  // Read tuples. Last parameter defines the batch size that will be returned when calling nextResult()
  if (pScan_op->readTuples(NdbOperation::LM_CommittedRead, 0, 0, 1) != 0){
    std::cout << pTransaction->getNdbError().message << std::endl;
    mNdbConnection->closeTransaction(pTransaction);
    return false;
  }

  // Keep only the tuples with the CD_ID
  NdbScanFilter filter(pScan_op);

  filter.begin(NdbScanFilter::AND);
  if (filter.cmp(NdbScanFilter::COND_EQ, column, preValue[column]) != 0) {
    LOG_NDB_API_ERROR(filter.getNdbError());
  }
  filter.end();

  // NOCommit since I'm only reading
  executeTransaction(pTransaction, NdbTransaction::NoCommit);

  bool toDelete = pScan_op->nextResult(true);
  mNdbConnection->closeTransaction(pTransaction);
  return toDelete;
}

void Cleaner::delEntries(NdbRecAttr* id, const char* table){

  const NdbDictionary::Dictionary* pDatabase = getDatabase(mNdbConnection);
  NdbTransaction* pTransaction = startNdbTransaction(mNdbConnection);
  const NdbDictionary::Index* pIndex= getIndex(pDatabase, table, "PRIMARY");

  NdbScanOperation* pScan_op = getNdbIndexScanOperation(pTransaction, pIndex);

  // Delete only the tuples with the CD_ID
  NdbScanFilter filter(pScan_op);

  filter.begin(NdbScanFilter::AND);
  if (filter.cmp(NdbScanFilter::COND_EQ, 0, id) != 0) {
    LOG_NDB_API_ERROR(filter.getNdbError());
  }
  filter.end();

  pScan_op->readTuples(NdbOperation::LM_Exclusive);

  executeTransaction(pTransaction, NdbTransaction::NoCommit);

  int check;
  while((check = pScan_op->nextResult(true)) == 0){
      do {
        LOG_INFO("inner loop");
      	pScan_op->deleteCurrentTuple();
      } while((check = pScan_op->nextResult(false)) == 0);

      if(check != -1){
        executeTransaction(pTransaction, NdbTransaction::Commit);
      }
  }

  mNdbConnection->closeTransaction(pTransaction);
}
