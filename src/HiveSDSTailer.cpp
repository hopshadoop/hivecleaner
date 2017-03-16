/*
 * Copyright (C) 2016 Hops.io
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
 * File:   HiveSDSTailer.h
 * Author: Fabio Buso <buso@kth.se>
 *
 */

#include "HiveSDSTailer.h"

using namespace Utils;
using namespace Utils::NdbC;

const string sds_table = "SDS";
const int sds_noCols = 12;
const string sds_cols[sds_noCols] =
    {
      "SD_ID",
      "CD_ID",
      "INPUT_FORMAT",
      "IS_COMPRESSED",
      "IS_STOREDASSUBDIRECTORIES",
      "LOCATION",
      "NAME",
      "NUM_BUCKETS",
      "OUTPUT_FORMAT",
      "PARENT_ID",
      "PARTITION_ID",
      "SERDE_ID"
    };

enum column_idx{
  SD_ID,
  CD_ID
};

const int sds_noEvents = 1;
const NdbDictionary::Event::TableEvent sds_events[sds_noEvents] = {NdbDictionary::Event::TE_DELETE};

const WatchTable HiveSDSTailer::TABLE = {sds_table, sds_cols, sds_noCols, sds_events, sds_noEvents};

const string cds_table = "CDS";

HiveSDSTailer::HiveSDSTailer(Ndb* ndb, const int poll_maxTimeToWait)
  :TableTailer(ndb, TABLE, poll_maxTimeToWait) { }

void HiveSDSTailer::handleEvent(NdbDictionary::Event::TableEvent eventType, NdbRecAttr* preValue[], NdbRecAttr* value[]){
  LOG_INFO("Event received. Primary Key value: " << preValue[SD_ID]->int32_value() << " CD_ID " << preValue[CD_ID]->int32_value());

  // Check that there are no more StorageDescriptors referencing the ColumnDescriptor_ID
  const NdbDictionary::Dictionary* pDatabase = getDatabase(mNdbConnection);
  const NdbDictionary::Table* pTable = getTable(pDatabase, sds_table.c_str());

  // Define transaction
  NdbTransaction* pTransaction = startNdbTransaction(mNdbConnection);

  // Get scan operation
  NdbScanOperation* pScan_op = getNdbScanOperation(pTransaction, pTable);

  // Read tuples. Last parameter defines the batch size that will be returned when calling nextResult()
  if (pScan_op->readTuples(NdbOperation::LM_CommittedRead, 0, 0, 1) != 0){
    std::cout << pTransaction->getNdbError().message << std::endl;
    mNdbConnection->closeTransaction(pTransaction);
    return;
  }

  // Keep only the tuples with the CD_ID
  NdbScanFilter filter(pScan_op);
  filter.eq(CD_ID, static_cast<Uint64>(preValue[CD_ID]->int64_value()));

  // NOCommit since I'm only reading
  executeTransaction(pTransaction, NdbTransaction::NoCommit);

  if (pScan_op->nextResult(true) == 1) {
    deleteCDID(preValue[CD_ID]);
  }

  mNdbConnection->closeTransaction(pTransaction);
}

void HiveSDSTailer::deleteCDID(NdbRecAttr* id){
  LOG_INFO("deleteCDID " << id);

  const NdbDictionary::Dictionary* pDatabase = getDatabase(mNdbConnection);
  NdbTransaction* pTransaction = startNdbTransaction(mNdbConnection);
  const NdbDictionary::Index* pIndex= getIndex(pDatabase, cds_table.c_str(), "PRIMARY");

  NdbScanOperation* pScan_op = getNdbIndexScanOperation(pTransaction, pIndex);

  // Keep only the tuples with the CD_ID
  NdbScanFilter filter(pScan_op);

  filter.begin(NdbScanFilter::OR);
  if (filter.cmp(NdbScanFilter::COND_EQ, 0, id) != 0) {
      LOG_NDB_API_ERROR(filter.getNdbError());
  }
  filter.end();

  pScan_op->readTuples(NdbOperation::LM_Exclusive);

  executeTransaction(pTransaction, NdbTransaction::NoCommit);

  int check;
  while((check = pScan_op->nextResult(true)) == 0){
      LOG_INFO("outer loop");
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

HiveSDSTailer::~HiveSDSTailer() { }
