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
 * File:   main.cpp
 * Author: Mahmoud Ismail<maism@kth.se>
 *
 */
#include "Notifier.h"
#include <boost/program_options.hpp>
#include "Version.h"

namespace po = boost::program_options;

int main(int argc, char** argv) {

    string connection_string;
    string metastore_name = "metastore";
    string hopsfs_name = "hops";
    string scratchdir_path = "/tmp/hive";
    int poll_maxTimeToWait = 2000;
    int log_level = 2;
    int scratchdir_exp = 168;

    bool stats = false;

    po::options_description desc("Allowed options");
    desc.add_options()
            ("help", "produce help message")
            ("connection", po::value<string>(&connection_string), "connection string/ ndb host")
            ("metastore", po::value<string>(&metastore_name)->default_value(metastore_name), "Hive metastore database name.")
            ("hops", po::value<string>(&hopsfs_name)->default_value(hopsfs_name), "HopsFS database name.")
            ("poll_maxTimeToWait", po::value<int>(&poll_maxTimeToWait)->default_value(poll_maxTimeToWait), "max time to wait in miliseconds while waiting for events in pollEvents")
            ("scratchdir_path", po::value<string>(&scratchdir_path)->default_value(scratchdir_path), "Path of a scratch directory for Hive Tez")
            ("scratchdir_exp", po::value<int>(&scratchdir_exp)->default_value(scratchdir_exp), "Expiration time for scratchdir in hours")
            ("log_level", po::value<int>(&log_level)->default_value(log_level), "log level trace=0, debug=1, info=2, warn=3, error=4, fatal=5")
            ("stats", po::value<bool>(&stats)->default_value(stats), "enable or disable print of accumulators stats")
            ("version", "ePipe version")
            ;

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    if (vm.count("help")) {
        cout << desc << endl;
        return EXIT_SUCCESS;
    }

    if (vm.count("version")) {
        cout << "ePipe " << HIVECLEANER_VERSION_MAJOR << "."  << HIVECLEANER_VERSION_MINOR
                << "." << HIVECLEANER_VERSION_BUILD << endl;
        return EXIT_SUCCESS;
    }

    Logger::setLoggerLevel(log_level);

    if(connection_string.empty() || metastore_name.empty() || hopsfs_name.empty() || scratchdir_path.empty()){
        LOG_ERROR("you should provide at least connection, metastore database, hopsfs database and the path of the Hive scratchdir");
        return EXIT_FAILURE;
    }

    // Create no const char * to be parsed during inode resolution

    Notifier *notifer = new Notifier(connection_string.c_str(), metastore_name.c_str(), hopsfs_name.c_str(), scratchdir_path, poll_maxTimeToWait, scratchdir_exp);
    notifer->start();

    return EXIT_SUCCESS;
}
