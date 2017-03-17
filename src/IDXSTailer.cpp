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
 * File:   IDXSTailer.cpp
 * Author: Fabio Buso <buso@kth.se>
 *
 */

#include "IDXSTailer.h"
#include "array_adapter.hpp"

using namespace Utils;
using namespace Utils::NdbC;

const string idxs_table = "IDXS";
const int idxs_noCols = 9;
const string idxs_cols[idxs_noCols] =
    {
      "INDEX_ID",
      "CREATE_TIME",
      "DEFERRED_REBUILD",
      "INDEX_HANDLER_CLASS",
      "INDEX_NAME",
      "INDEX_TBL_ID",
      "LAST_ACCESS_TIME",
      "ORIG_TBL_ID",
      "SD_ID"
    };

const int sd_id = 8;

const int idxs_noEvents = 1;
const NdbDictionary::Event::TableEvent idxs_events[idxs_noEvents] = {NdbDictionary::Event::TE_DELETE};

const WatchTable IDXSTailer::TABLE = {idxs_table, idxs_cols, idxs_noCols, idxs_events, idxs_noEvents};

const string sds_table = "SDS";
const string tbls_table = "TBLS";

IDXSTailer::IDXSTailer(Ndb* ndb, const int poll_maxTimeToWait)
  :Cleaner(ndb, TABLE, poll_maxTimeToWait) { }

void IDXSTailer::handleEvent(NdbDictionary::Event::TableEvent eventType, NdbRecAttr* preValue[], NdbRecAttr* value[]){
  LOG_INFO("Index delete event received. Primary Key value: " << preValue[0]->u_64_value());

  // Delete SDS entry related to the index
  delEntries(preValue[sd_id], sds_table.c_str());

  // Delete the index directory on Hdfs.
  // Wait 2 minutes to avoid collisions with Hive drop (table/index)
  const char* path = getHdfsIndexPath(preValue[5]);
}

const char* IDXSTailer::getHdfsIndexPath(NdbRecAttr* tbl_id) {

  const NdbDictionary::Dictionary* pDatabase = getDatabase(mNdbConnection);
  const NdbDictionary::Index* pTblIndex= getIndex(pDatabase, tbls_table, "PRIMARY");

  // Define transaction
  NdbTransaction* pTransaction = startNdbTransaction(mNdbConnection);

  // Get scan operation
  NdbScanOperation* pTblScan_op = getNdbIndexScanOperation(pTransaction, pTblIndex);

  // Read tuples. Last parameter defines the batch size that will be returned when calling nextResult()
  if (pTblScan_op->readTuples(NdbOperation::LM_CommittedRead, 0, 0, 1) != 0){
    std::cout << pTransaction->getNdbError().message << std::endl;
    mNdbConnection->closeTransaction(pTransaction);
    return NULL;
  }

  // Keep only the tuples with the CD_ID
  NdbScanFilter filter(pTblScan_op);
  filter.begin(NdbScanFilter::AND);
  if (filter.cmp(NdbScanFilter::COND_EQ, 0, tbl_id) != 0) {
    LOG_NDB_API_ERROR(filter.getNdbError());
  }
  filter.end();

  // Retrieve SD_ID
  NdbRecAttr* tbl_sd_id = getNdbOperationValue(pTblScan_op, "SD_ID");

  // NOCommit since I'm only reading
  executeTransaction(pTransaction, NdbTransaction::NoCommit);

  if (pTblScan_op->nextResult(true) == 0) {
    LOG_INFO("-----TABLE FOUND");
    // The index table is still to be deleted. Get the path.
    const NdbDictionary::Index* pSdsIndex= getIndex(pDatabase, sds_table, "PRIMARY");
    NdbScanOperation* pSdsScan_op = getNdbIndexScanOperation(pTransaction, pSdsIndex);

    if (pSdsScan_op->readTuples(NdbOperation::LM_CommittedRead, 0, 0, 1) != 0){
      std::cout << pTransaction->getNdbError().message << std::endl;
      mNdbConnection->closeTransaction(pTransaction);
      return NULL;
    }

    // Keep only the tuples with the CD_ID
    NdbScanFilter filter(pSdsScan_op);
    filter.begin(NdbScanFilter::AND);
    if (filter.cmp(NdbScanFilter::COND_EQ, 0, tbl_sd_id) != 0) {
      LOG_NDB_API_ERROR(filter.getNdbError());
    }
    filter.end();

    // Retrieve SD_ID
    NdbRecAttr* index_path = getNdbOperationValue(pSdsScan_op, "LOCATION");

    // NOCommit since I'm only reading
    executeTransaction(pTransaction, NdbTransaction::NoCommit);

    if (pSdsScan_op->nextResult(true) == 0) {
      // return the LOCATION
      LOG_INFO("----LOCATION FOUND");
      ReadOnlyArrayAdapter attr_adapter;
      ReadOnlyArrayAdapter::ErrorType error;
      string value = attr_adapter.get_string(index_path, error);
      LOG_INFO(value);

      mNdbConnection->closeTransaction(pTransaction);
      return NULL;
    }
  }

  mNdbConnection->closeTransaction(pTransaction);
  return NULL;
}

IDXSTailer::~IDXSTailer() { }
