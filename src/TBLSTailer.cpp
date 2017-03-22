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
 * File:   TBLSTailer.cpp
 * Author: Fabio Buso <buso@kth.se>
 *
 */

#include "TBLSTailer.h"

using namespace Utils;
using namespace Utils::NdbC;

const string tbls_table = "TBLS";
const int tbls_noCols = 12;
const string tbls_cols[tbls_noCols] =
    {
      "TBL_ID",
      "CREATE_TIME",
      "DB_ID",
      "LAST_ACCESS_TIME",
      "OWNER",
      "RETENTION",
      "IS_REWRITE_ENABLED",
      "SD_ID",
      "TBL_NAME",
      "TBL_TYPE",
      "VIEW_EXPANDED_TEXT",
      "VIEW_ORIGINAL_TEXT"
     };

enum column_idx{
  TBL_ID,
  SD_ID=7
};

const int tbls_noEvents = 1;
const NdbDictionary::Event::TableEvent tbls_events[tbls_noEvents] = {NdbDictionary::Event::TE_DELETE};

const WatchTable TBLSTailer::TABLE = {tbls_table, tbls_cols, tbls_noCols, tbls_events, tbls_noEvents};

const string sds_table = "SDS";

TBLSTailer::TBLSTailer(Ndb* ndb, const int poll_maxTimeToWait)
  :Cleaner(ndb, TABLE, poll_maxTimeToWait) { }

void TBLSTailer::handleEvent(NdbDictionary::Event::TableEvent eventType, NdbRecAttr* preValue[], NdbRecAttr* value[]){
  LOG_INFO("Delete TBLS event received. Primary Key value: " << preValue[TBL_ID]->u_64_value());

  delEntries(preValue[SD_ID], sds_table.c_str());
}

TBLSTailer::~TBLSTailer() { }
