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
 * File:   SDSTailer.cpp
 * Author: Fabio Buso <buso@kth.se>
 *
 */

#include "SDSTailer.h"

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
  CD_ID,
  SERDE_ID=11,
};

const int sds_noEvents = 1;
const NdbDictionary::Event::TableEvent sds_events[sds_noEvents] = {NdbDictionary::Event::TE_DELETE};

const WatchTable SDSTailer::TABLE = {sds_table, sds_cols, sds_noCols, sds_events, sds_noEvents};

const string cds_table = "CDS";
const string serdes_table = "SERDES";

SDSTailer::SDSTailer(Ndb* ndb, const int poll_maxTimeToWait)
  :Cleaner(ndb, TABLE, poll_maxTimeToWait) { }

void SDSTailer::handleEvent(NdbDictionary::Event::TableEvent eventType, NdbRecAttr* preValue[], NdbRecAttr* value[]){
  LOG_INFO("Delete SDS event received. Primary Key value: " << preValue[SD_ID]->u_64_value());

  if (check(preValue, CD_ID, sds_table.c_str())) {
    delEntries(preValue[CD_ID], cds_table.c_str());
  }

  // SDS is defined as dependent in package.jdo. This means that when the SD is deleted, also the SERDE entry goes away.
  // There is no possibility of duplicates.
  delEntries(preValue[SERDE_ID], serdes_table.c_str());
}

SDSTailer::~SDSTailer() { }
