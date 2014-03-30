// The MIT License (MIT)
//
// Slowkicker v0.1 Copyright (c) 2014 Biohazard
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

#include <cstdio>
#include <cctype>
#include <cstdlib>
#include <iostream>
#include <cstring>
#include <cerrno>
#include <ctime>
#include <csignal>
#include <cstdarg>
#include <string>
#include <sstream>
#include <deque>
#include <sys/time.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <unistd.h>
#include <fnmatch.h>
#include "glconf.h"

struct Directory
{
    const char* mask;
    double minSpeed;
    std::time_t minDuration;
    int maxKicks;
};

const char*       GLFTPD_ROOT   = "/glftpd";
const char*       LOG_FILE      = "/glftpd/ftp-data/logs/slowkicker.log";
const char*       LOCK_FILE     = "/glftpd/tmp/slowkicker.lock";
const key_t       IPC_KEY       = 0xDEADBABE;
bool              ONCE_ONLY     = true;
Directory         DIRECTORIES[] = {
    { "/site/iso/*",        75,    /* kB/s */    15,    /* seconds */  3 },
    { "/site/mp3/*",        75,    /* kB/s */    15,    /* seconds */  3 },
    { "/site/0day/*",       75,    /* kB/s */    15,    /* seconds */  3 }
};

struct History
{
    std::string username;
    std::string path;
    int numKicks;
};

std::deque<History> history;
const std::size_t maxHistory = 1000;

History* getHistory(const char* username, const char* path)
{
    for (std::size_t i = 0; i < history.size(); ++i) {
        if (history[i].username == username && history[i].path == path) {
            return &history[i];
        }
    }

    return NULL;
}

int getNumKicks(const char* username, const char* path)
{
    History* entry = getHistory(username, path);
    if (entry == NULL) {
        return 0;
    }

    return entry->numKicks;
}

void incrNumKicks(const char* username, const char* path)
{
    History* entry = getHistory(username, path);
    if (entry == NULL) {
        History entry = { username, path, 1 };
        history.push_front(entry);
        if (history.size() >= maxHistory) {
            history.pop_back();
        }
        return;
    }

    ++entry->numKicks;
}

const Directory* getDirectory(const char* path)
{
    static const std::size_t numDirectories = sizeof(DIRECTORIES) / sizeof(Directory);

    for (std::size_t i = 0; i < numDirectories; ++i) {
        if (!fnmatch(DIRECTORIES[i].mask, path, 0)) {
            return &DIRECTORIES[i];
        }
    }

    return NULL;
}

std::string formatTimestamp()
{
    const std::time_t now = std::time(NULL);
    char timestamp[26];
    std::strftime(timestamp, sizeof(timestamp), "%a %b %e %T %Y", std::localtime(&now));
    return timestamp;
}

void gllog(const char* username, const char* path, double speed)
{
    std::string logPath = GLFTPD_ROOT + std::string("/ftp-data/logs/glftpd.log");
    FILE* f = std::fopen(logPath.c_str(), "a");
    if (f == NULL) {
        return;
    }

    std::fprintf(f, "%s SLOWKICK: \"%s\" \"%s\" \"%.0f\"\n",
                 formatTimestamp().c_str(), path, username, speed);

    std::fclose(f);
}

void log(const char* format,...)
{
    FILE* f = std::fopen(LOG_FILE, "a");
    if (f == NULL) {
        return;
    }

    std::fprintf(f, "%s ", formatTimestamp().c_str());

    va_list args;
    va_start(args, format);
    vfprintf(f, format, args);
    va_end(args);

    std::fprintf(f, "\n");
    std::fclose(f);
}

std::string buildPath(const ONLINE& online)
{
    std::string realPath(GLFTPD_ROOT);
    realPath += online.currentdir;

    struct stat st;
    if (stat(realPath.c_str(), &st) < 0) {
        if (errno != ENOENT) {
            log("Unable to stat path: %s: %s", online.currentdir, strerror(errno));
        }
        return "";
    }

    std::string path(online.currentdir);
    if (S_ISDIR(st.st_mode)) {
        const char* filename = online.status + 5;
        if (filename == '\0') {
            log("Malformed status: %s", online.status);
            return "";
        }
        path += '/';
        path += filename;
        while (!std::isprint(path[path.size() - 1])) {
            path.resize(path.size() - 1);
        }
    }

    return path;
}

void undupe(const char* username, const char* path)
{
    const char* filename = strrchr(path, '/');
    if (filename == NULL) {
        log("Undupe failed, malformed path: %s: %s", username, path);
        return;
    }
    ++filename;

    std::ostringstream command;
    command << GLFTPD_ROOT << "/bin/undupe"
            << " -u " << username
            << " -f '" << filename << "'"
            << " >/dev/null 2>/dev/null";

    system(command.str().c_str());
}

bool isUploading(const ONLINE& online)
{
    return online.procid != 0 &&
           strncasecmp(online.status, "STOR ", 5) == 0 &&
           kill(online.procid, 0) == 0;
}

bool slowKickCheck(const ONLINE& online, const std::string& path, double& speed)
{
    const Directory* directory = getDirectory(path.c_str());
    if (directory == NULL) {
        return false;
    }

    struct timeval now;
    gettimeofday(&now, NULL);

    double duration = (now.tv_sec - online.tstart.tv_sec) +
                      ((now.tv_usec - online.tstart.tv_usec) / 1000000.0);
    speed = (duration == 0 ? online.bytes_xfer
                           : online.bytes_xfer / duration) / 1024.0;
    if (duration < directory->minDuration || speed >= directory->minSpeed) {
        return false;
    }

    if (getNumKicks(online.username, path.c_str()) >= directory->maxKicks) {
        return false;
    }

    return true;
}

bool kick(const ONLINE& online, const std::string& path, double speed)
{
    if (kill(online.procid, SIGTERM) < 0) {
        if (errno != ESRCH) {
            log("Unable to kill process: %lld: %s", (long long) online.procid, strerror(errno));
        }
        return false;
    }

    const std::string realPath = GLFTPD_ROOT + path;

    struct stat st;
    if (stat(realPath.c_str(), &st) < 0) {
        if (errno != ENOENT) {
            log("Unable to stat path: %s: %s", realPath.c_str(), strerror(errno));
            return false;
        }

        if (st.st_uid != online.procid) {
            return false;
        }
    }

    if (unlink(realPath.c_str()) < 0) {
        log("Unable to delete file: %s: %s", realPath.c_str(), strerror(errno));
        return false;
    }

    undupe(online.username, path.c_str());

    log("Kicked user for slow uploading: %s: %.0fkB/s: %s", online.username, speed, path.c_str());
    gllog(online.username, path.c_str(), speed);

    return true;
}

void detach_online(ONLINE* online)
{
    shmdt(online);
}

ONLINE* open_online(std::size_t& num)
{
    long long shmid = shmget(IPC_KEY, 0, 0);
    if (shmid < 0) {
        if (errno == ENOENT) {
            return NULL;
        }
        log("Unable to open online users: shmget: %s", strerror(errno));
        return NULL;
    }

    ONLINE* online = (ONLINE*) shmat(shmid, NULL, SHM_RDONLY);
    if (online == (ONLINE*) -1) {
        log("Unable to open online users: shmat: %s", strerror(errno));
        return NULL;
    }

    struct shmid_ds	stat;
    if (shmctl(shmid, IPC_STAT, &stat) < 0) {
        log("Unable to open online users: shmctl: %s", strerror(errno));
        detach_online(online);
        return NULL;
    }

    num = stat.shm_segsz / sizeof(ONLINE);
    return online;
}

void check()
{
    std::size_t numOnline;
    ONLINE* online = open_online(numOnline);
    if (online == NULL) {
        return;
    }

    for (std::size_t i = 0; i < numOnline; ++i) {
        if (isUploading(online[i])) {
            std::string path = buildPath(online[i]);
            if (!path.empty()) {
                double speed;
                if (slowKickCheck(online[i], path, speed)) {
                    if (kick(online[i], path, speed)) {
                        incrNumKicks(online[i].username, path.c_str());
                    }
                }
            }
        }
    }

    detach_online(online);
}

bool acquireLock()
{
    int fd = open(LOCK_FILE, O_CREAT | O_WRONLY, 0600);
    if (fd < 0) {
        std::cerr << "Unable to create/open lock file: " << strerror(errno) << std::endl;
        return false;
    }

    if (flock(fd, LOCK_EX | LOCK_NB) < 0) {
        if (errno == EWOULDBLOCK) {
            std::cerr << "Slowkicker is already running." << std::endl;
            return false;
        }

        std::cerr << "Unable to acquire exclusive lock: " << strerror(errno) << std::endl;
        return false;
    }

    return true;
}

int main()
{
    if (!acquireLock()) {
        return 1;
    }

    if (!fork()) {
        while (true) {
            check();
            sleep(1);
        }
    }
}
