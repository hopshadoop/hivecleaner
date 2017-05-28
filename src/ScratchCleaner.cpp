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
 * File:   ScratchCleaner.cpp
 * Author: Fabio Buso <buso@kth.se>
 */

#include <cstring>
#include <chrono>
#include "ScratchCleaner.h"

using namespace Utils;
using namespace Utils::NdbC;
using namespace std::chrono;

ScratchCleaner::ScratchCleaner(Ndb* ndb, string scratchDirPath, const int scratchDirExp, const int interval):
  mNdbConnection(ndb),
  mScratchDirPath(scratchDirPath),
  mScratchDirExp(scratchDirExp),
  mInterval(interval) {
    // Clean scratchdirs path from the trailig / if present
    if (mScratchDirPath.at(mScratchDirPath.length()-1) == '/') {
      mScratchDirPath = mScratchDirPath.substr(0, mScratchDirPath.length() - 1);
    }
}

ScratchCleaner::~ScratchCleaner() {
  LOG_INFO("Stopping ScratchCleaner...");
  if (mStarted) {
    mThread.interrupt();
  }

  delete mNdbConnection;
}

void ScratchCleaner::start() {
  if (mStarted) {
    return;
  }

  LOG_INFO("Starting ScratchCleaner...");
  mThread = boost::thread(&ScratchCleaner::run, this);
  mStarted = true;
}

list<string> ScratchCleaner::tokenizePath(string s) {
  list<string> tokens;
  int pos = 0;
  while ((pos = s.find("/")) != std::string::npos) {
      string token = s.substr(0, pos);
      tokens.push_back(token);
      s.erase(0, pos + 1);
  }
  tokens.push_back(s);
  return tokens;
}

void ScratchCleaner::run() {
  list<string> pathComponents = tokenizePath(mScratchDirPath);
  while(1) {
    // infinite loop
    // sleep for mInterval
    boost::this_thread::sleep_for(boost::chrono::seconds(mInterval));

    // Resolve recursively the inode of the scratchdir
    int32_t parentId = 1;
    for (auto it = pathComponents.begin(); it != pathComponents.end(); it++){
      if (it->empty()) {
        continue;
      }
      string dirName;
      if (it->length() < 255) {
        dirName = get_ndb_varchar(*it, NdbDictionary::Column::ArrayTypeShortVar);
      } else {
        dirName = get_ndb_varchar(*it, NdbDictionary::Column::ArrayTypeMediumVar);
      }
      parentId = findInodeId(dirName.c_str(), parentId);
    }

    if (parentId == 1) {
      LOG_FATAL("Cannot find scratch directory");
    }

    // Query the database for old scratchdirs
    system_clock::time_point tp = system_clock::now();
    system_clock::duration dtn = tp.time_since_epoch();

    long timeLimit = duration_cast<milliseconds>(dtn).count() - (mScratchDirExp * 3600000); // 1h = 3.6*10^5
    list<string> oldScratchDirs = getOldDirectories(parentId, timeLimit);
    for (auto it = oldScratchDirs.begin(); it != oldScratchDirs.end(); it++) {
      LOG_INFO(mScratchDirPath << "/" << *it);
      string path = mScratchDirPath + "/" + *it;
      hdfs_delete(path);
    }
  }
}

// Util function to resolve the inode id for the scratchdir path
int32_t ScratchCleaner::findInodeId(const char* dirName, int32_t parentId) {
  const NdbDictionary::Dictionary* pDatabase = getDatabase(mNdbConnection);
  const NdbDictionary::Table* pTable = getTable(pDatabase, "hdfs_inodes");

  // Start transaction
  NdbTransaction* pTransaction = startNdbTransaction(mNdbConnection);

  // Scan for inode_id
  NdbScanOperation* pScan_op = getNdbScanOperation(pTransaction, pTable);

  // Read tuples. Last parameter defines the batch size that will be returned when calling nextResult()
  if (pScan_op->readTuples(NdbOperation::LM_CommittedRead, 0, 0, 1) != 0){
    std::cout << pTransaction->getNdbError().message << std::endl;
    mNdbConnection->closeTransaction(pTransaction);
    LOG_FATAL("Error setting up the scan operation");
  }

  NdbScanFilter filter(pScan_op);
  filter.begin(NdbScanFilter::AND);
  //filter.cmp(NdbScanFilter::COND_EQ, 1, &parentId, 4);
  // name - 3rd column
  // parent_id - 2nd column
  // root inode_id = 1
  if (filter.cmp(NdbScanFilter::COND_EQ, 2, dirName, 257) != 0 ||
      filter.cmp(NdbScanFilter::COND_EQ, 1, &parentId, 4) != 0 ) {
    LOG_NDB_API_ERROR(filter.getNdbError());
  }
  filter.end();

  NdbRecAttr* inodeId;
  inodeId = pScan_op->getValue("id");

  // NoCommit, just reading
  executeTransaction(pTransaction, NdbTransaction::NoCommit);

  // Check if directory exists
  if (pScan_op->nextResult(true) != 0) {
    LOG_FATAL("Cannot find " << dirName << " directory");
  }

  pTransaction->close();
  return inodeId->int32_value();
}

// Find a list of scratchdir paths
list<string> ScratchCleaner::getOldDirectories(int32_t parentId, int64_t timeLimit) {
  const NdbDictionary::Dictionary* pDatabase = getDatabase(mNdbConnection);
  const NdbDictionary::Table* pTable = getTable(pDatabase, "hdfs_inodes");

  // Start transaction
  NdbTransaction* pTransaction = startNdbTransaction(mNdbConnection);
  NdbScanOperation* pScan_op = getNdbScanOperation(pTransaction, pTable);

  // Read tuples. Last parameter defines the batch size that will be returned when calling nextResult()
  if (pScan_op->readTuples(NdbOperation::LM_CommittedRead, 0, 0, 1) != 0){
    std::cout << pTransaction->getNdbError().message << std::endl;
    mNdbConnection->closeTransaction(pTransaction);
    LOG_FATAL("Error setting up the scan operation");
  }

  NdbScanFilter filter(pScan_op);
  filter.begin(NdbScanFilter::AND);
  //filter.cmp(NdbScanFilter::COND_EQ, 1, &parentId, 4);
  // modification_time - 6th column
  // parent_id - 2nd column
  // root inode_id = 1
  if (filter.cmp(NdbScanFilter::COND_LE, 6, &timeLimit, 8) != 0 ||
      filter.cmp(NdbScanFilter::COND_EQ, 1, &parentId, 4) != 0 ) {
    LOG_NDB_API_ERROR(filter.getNdbError());
  }
  filter.end();

  NdbRecAttr* inodeId;
  inodeId = pScan_op->getValue("name");

  // Retrieve component name
  NdbRecAttr* scratchdir_name = getNdbOperationValue(pScan_op, "name");

  // NoCommit, just reading
  executeTransaction(pTransaction, NdbTransaction::NoCommit);

  // Get List of old scratchdirs
  list<string> oldScratchdir_names;
  while(pScan_op->nextResult(true) == 0){
      do {
        string value = get_string(scratchdir_name);
        oldScratchdir_names.push_back(value);
      } while(pScan_op->nextResult(false) == 0);
  }

  pTransaction->close();
  return oldScratchdir_names;
}
