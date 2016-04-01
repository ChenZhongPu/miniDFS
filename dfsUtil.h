//
// Created by ChenZhongPu on 3/31/16.
//

#ifndef MINIDFS_DFSUTIL_H
#define MINIDFS_DFSUTIL_H
#include <sstream>
#include <string>
#include <vector>

std::vector<std::string> &split(const std::string &s, char delim, std::vector<std::string> &elems);

std::vector<std::string> split(const std::string &s, char delim);


struct fileRange
{
    long long from;
    int count;
    int blockId;
    fileRange(long long _from, int _count, int _blockId)
    {
        from = _from;
        count = _count;
        blockId = _blockId;
    }
};

#endif //MINIDFS_DFSUTIL_H
