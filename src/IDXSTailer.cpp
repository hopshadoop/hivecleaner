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

// If the table has been deleted from the fs the index information will be deleted from the db
// however the index directory will remain on the fs. Users will have to delete it from the fs as well.
void IDXSTailer::handleEvent(NdbDictionary::Event::TableEvent eventType, NdbRecAttr* preValue[], NdbRecAttr* value[]){
  LOG_INFO("Delete IDXS event received. Primary Key value: " << preValue[0]->u_64_value());

  // Delete SDS entry related to the index
  delEntries(preValue[sd_id], sds_table.c_str());
}

IDXSTailer::~IDXSTailer() { }
