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
 * File:   Notifier.cpp
 * Author: Fabio Buso <buso@kth.se>
 *
 */

#include "Utils.h"
#include "Notifier.h"

Notifier::Notifier(const char* connection_string, const char* metastore_name,
  const char* hopsfs_name, string scratchdirs_path,  const int poll_maxTimeToWait, const int scratchdir_exp, const bool recovery)
    : mMetastoreName(metastore_name), mHopsfsName(hopsfs_name), mPollMaxTimeToWait(poll_maxTimeToWait), mRecovery(recovery), mStracthdirs_path(scratchdirs_path),
    mScratchdir_exp(scratchdir_exp){
    mClusterConnection = connect_to_cluster(connection_string);
    setup();
}

void Notifier::start() {
    LOG_INFO("hiveCleaner starting...");
    ptime t1 = Utils::getCurrentTime();

    mSDSTailer->start(mRecovery);
    mSkvTailer->start(mRecovery);
    mSklTailer->start(mRecovery);
    mIdxsTailer->start(mRecovery);
    mTblsTailer->start(mRecovery);
    mPartTailer->start(mRecovery);
    mScratchCleaner->start();

    ptime t2 = Utils::getCurrentTime();
    LOG_INFO("hiveCleaner started in " << Utils::getTimeDiffInMilliseconds(t1, t2) << " msec");
    mSDSTailer->waitToFinish();
    mSkvTailer->waitToFinish();
    mSklTailer->waitToFinish();
    mIdxsTailer->waitToFinish();
    mTblsTailer->waitToFinish();
    mPartTailer->waitToFinish();
}

void Notifier::setup() {
    Ndb* sds_tailer_connection = create_ndb_connection(mMetastoreName);
    Ndb* skv_tailer_connection = create_ndb_connection(mMetastoreName);
    Ndb* skl_tailer_connection = create_ndb_connection(mMetastoreName);
    Ndb* idxs_tailer_connection = create_ndb_connection(mMetastoreName);
    Ndb* tbls_tailer_connection = create_ndb_connection(mMetastoreName);
    Ndb* part_tailer_connection = create_ndb_connection(mMetastoreName);
    Ndb* scratch_cleaner_connection = create_ndb_connection(mHopsfsName);
    mSDSTailer = new SDSTailer(sds_tailer_connection, mPollMaxTimeToWait);
    mSkvTailer = new SkewedValuesTailer(skv_tailer_connection, mPollMaxTimeToWait);
    mSklTailer = new SkewedLocTailer(skl_tailer_connection, mPollMaxTimeToWait);
    mIdxsTailer = new IDXSTailer(idxs_tailer_connection, mPollMaxTimeToWait);
    mTblsTailer = new TBLSTailer(tbls_tailer_connection, mPollMaxTimeToWait);
    mPartTailer = new PARTTailer(part_tailer_connection, mPollMaxTimeToWait);
    mScratchCleaner = new ScratchCleaner(scratch_cleaner_connection, mStracthdirs_path, mScratchdir_exp, 86400);
}

Ndb_cluster_connection* Notifier::connect_to_cluster(const char *connection_string) {
    Ndb_cluster_connection* c;

    if (ndb_init())
        exit(EXIT_FAILURE);

    LOG_INFO("Connection string " << connection_string);
    c = new Ndb_cluster_connection(connection_string);

    if (c->connect(RETRIES, DELAY_BETWEEN_RETRIES, VERBOSE)) {
        fprintf(stderr, "Unable to connect to cluster.\n\n");
        exit(EXIT_FAILURE);
    }

    if (c->wait_until_ready(WAIT_UNTIL_READY, WAIT_UNTIL_READY) < 0) {

        fprintf(stderr, "Cluster was not ready.\n\n");
        exit(EXIT_FAILURE);
    }

    return c;
}

Ndb* Notifier::create_ndb_connection(const char* database) {
    Ndb* ndb = new Ndb(mClusterConnection, database);
    if (ndb->init() == -1) {
        LOG_NDB_API_ERROR(ndb->getNdbError());
    }

    return ndb;
}

Notifier::~Notifier() {
    delete mSDSTailer;
    delete mSkvTailer;
    delete mSklTailer;
    delete mIdxsTailer;
    delete mTblsTailer;
    delete mPartTailer;
    delete mScratchCleaner;
    ndb_end(2);
}
