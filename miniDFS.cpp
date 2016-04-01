//
// Created by ChenZhongPu on 3/31/16.
//
#include <iostream>
#include <fstream>
#include <string>
#include <utility>
#include <cmath>
#include <vector>
#include <map>
#include <algorithm>
#include <thread>
#include <mutex>
#include <condition_variable>

#include <boost/filesystem.hpp>
#include <boost/serialization/map.hpp>
#include <boost/serialization/utility.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>

#include "dfsUtil.h"

using namespace std;

const int max_dataserver_num = 20;

// 2MB
constexpr double block_size = 2.0 * 1024 * 1024;

constexpr int block_size_int = 2 * 1024 * 1024;

int dataserver_num = 4;

int replicate_num = 3;

std::condition_variable cs_cv;
std::mutex cs_m;

std::condition_variable nd_cv;
std::mutex nd_m;

bool nameNotified = false;
bool processed = false;

vector<bool> dataNotified(max_dataserver_num, false);


bool ispropercmd;

enum class MsgType {put,read,hello};

enum class MetaType {id_file, file_block, block_server, current_id};

MsgType type;
int fileID = 0;

int read_fileId;

long long read_offset;
int read_count;

int server_executing_read;
string read_logic_file;
int read_block;
int offset_in_block;
bool is_ready_read;

string desFileName = "";

ifstream ifs;

// for therad communication: <serverid, range{from, count}>
// note that one serverid can hold server ranges
multimap<int, fileRange> server_fileRangesMap;


/**
 * metadata:
 *
比如, /home/a.txt 可能被分为3个block,需要知道每个block在哪?该block对应实际存储名
a-part0在0, 1; a-part1在1,2; a-part2在 2,3

<"/home/a.txt", "a.txt-part0, a.txt-part1">
<"a.txt-part0", "1, 2, 3" >

 */

// for metadata: <fildId, [logicFilePath, length]>
map<int, pair<string, long long>> fileid_path_lenMap;

// for metadata: <logicFilePath, blockfiles>
map<string, vector<string>> logicFile_BlockFileMap;

// for metadata: <blockfile, serverid>
map<string, vector<int>> block_serversMap;

bool processCmd(const string& cmd)
{
    string possibleCmds[] = {"put", "read", "hello"};
    vector<string> x = split(cmd, ' ');
    if (x[0].compare(possibleCmds[0]) == 0)
    {
        type = MsgType::put;
        if (x.size() != 2)
        {
            cerr << "Usage: put source_file_path" << endl;
            return false;
        }
        string sourcePath = x[1];
        ifs.open(sourcePath, std::ifstream::binary);
        if (! ifs.is_open())
        {
            cerr << "Error: the input file does not exist" << endl;
            return false;
        }

        boost::filesystem::path p(sourcePath);

        desFileName += p.filename().string();

    }
    else if (x[0].compare(possibleCmds[1]) == 0)
    {
        type = MsgType::read;
        if (x.size() != 4)
        {
            cerr << "Usage: read file_id offset count" << endl;
            return false;
        }

        read_fileId = stoi(x[1]);
        read_offset = stoi(x[2]);
        read_count = stoi(x[3]);
    }
    else if(x[0].compare(possibleCmds[2]) == 0)
    {
        type = MsgType::hello;
    }
    return true;
}


class NameServer
{
public:
    void operator() () const
    {
        loadMeta();

        for (;;)
        {
            unique_lock<mutex> cs_lk(cs_m);
            cs_cv.wait(cs_lk, []{return nameNotified;});

            cout << "name notified \n";

            if (ispropercmd && type == MsgType::put)
            {
                generateSplitInfo();
                // generate the info how to distibute file

            }
            else if (ispropercmd && type == MsgType::read)
            {
                // assign read work
                is_ready_read = assignReadWork();
            }

            nameNotified = false;

            fill_n(dataNotified.begin(), dataserver_num, true);

            nd_cv.notify_all();

            unique_lock<mutex> nd_lk(nd_m);
            nd_cv.wait(nd_lk, []{
                return all_of(dataNotified.begin(),
                              dataNotified.begin() + dataserver_num,
                              [](bool b){return !b;});
            });
            nd_lk.unlock();

            processed = true;
            cs_lk.unlock();
            cs_cv.notify_all();
        }
    }


private:
    void generateSplitInfo() const
    {
        ifs.seekg (0, ifs.end);
        long long length = ifs.tellg();
        ifs.seekg (0, ifs.beg);

        int blockNum = (int)ceil(length / block_size);

        // metadata: key=fileId, value=logicFilePath
        fileid_path_lenMap.emplace(make_pair(fileID, make_pair(desFileName, length)));

        fileID++;

        vector<string> blockPathList;
        for (int i = 0; i < blockNum; i++)
        {
            blockPathList.push_back(desFileName + "-part" + to_string(i));
        }
        logicFile_BlockFileMap.emplace(make_pair(desFileName, blockPathList));

        for (int i = 0; i < blockNum; i++)
        {

            vector<int> _serverids;
            _serverids.push_back(i % dataserver_num);

            fileRange range(i * block_size_int, 0, i);

            if (i == blockNum - 1)
            {
                range.count = length - i * block_size_int;
            }
            else
            {
                range.count = block_size_int;
            }
            server_fileRangesMap.emplace(make_pair(i % dataserver_num, range));
            // still should crate another two replications
            for (int j = 1; j < replicate_num; j++)
            {
                server_fileRangesMap.emplace(make_pair((i + j) % dataserver_num, range));

                _serverids.push_back((i + j) % dataserver_num);
            }

            block_serversMap.emplace(make_pair(desFileName + "-part" + to_string(i),
               _serverids));
        }

        // write metadata info to file
        writeMeta(MetaType::id_file);
        writeMeta(MetaType::file_block);
        writeMeta(MetaType::block_server);
        writeMeta(MetaType::current_id);
    }


    bool assignReadWork() const
    {
        // get the file info
        auto file_info = fileid_path_lenMap.find(read_fileId);
        if (file_info == fileid_path_lenMap.end())
        {
            cerr << "No such file with id = " << read_fileId << endl;
            return false;
        }
        else
        {
            cout << "find id = " << read_fileId << endl;

            string logic_file_name = file_info->second.first;
            long long total_len = file_info->second.second;

            if (read_offset + read_count > total_len)
            {
                cerr << "The expected reading exceeds"<< endl;
                return false;
            }

            int startblock = int(floor(read_offset / block_size));

            int spaceLeftOfThisBlock = int((startblock + 1) * block_size_int - read_offset);

            if (spaceLeftOfThisBlock < read_count)
            {
                // we assume that cannot read accoss blocks
                cerr << "Cannot read accoss blocks"<< endl;
                return false;
            }

            read_block = startblock;

            auto server_id_ite = block_serversMap.find(logic_file_name + "-part" + to_string(startblock));

            server_executing_read = server_id_ite->second[0];

            offset_in_block = int(read_offset - startblock * block_size_int);

            read_logic_file = logic_file_name;

        }
        return true;
    }

    void loadMeta() const
    {
        ifstream id_file_meta("dfsfiles/namenode/id-logicpath-meta");
        if(!id_file_meta)
        {
            return;
        }
        boost::archive::text_iarchive id_file_ov(id_file_meta);
        id_file_ov >> fileid_path_lenMap;
        id_file_meta.close();

        ifstream file_block_meta("dfsfiles/namenode/logicpath-blocks-meta");
        boost::archive::text_iarchive file_block_ov(file_block_meta);
        file_block_ov >> logicFile_BlockFileMap;
        file_block_meta.close();

        ifstream block_server_meta("dfsfiles/namenode/block-servers-meta");
        boost::archive::text_iarchive block_server_ov(block_server_meta);
        block_server_ov >> block_serversMap;
        block_server_meta.close();

        ifstream currentId_meta("dfsfiles/namenode/current-id-meta");
        string currenetId;
        getline(currentId_meta, currenetId);
        fileID = stoi(currenetId);
        currentId_meta.close();

    }

    void writeMeta(MetaType type) const
    {
        if (type == MetaType::id_file)
        {
            std::ofstream metaOfs("dfsfiles/namenode/id-logicpath-meta");
            boost::archive::text_oarchive ov(metaOfs);
            ov << fileid_path_lenMap;
            metaOfs.close();
        }
        else if (type == MetaType::file_block)
        {
            std::ofstream metaOfs("dfsfiles/namenode/logicpath-blocks-meta");
            boost::archive::text_oarchive ov(metaOfs);
            ov << logicFile_BlockFileMap;
            metaOfs.close();
        }
        else if (type == MetaType::block_server)
        {
            std::ofstream metaOfs("dfsfiles/namenode/block-servers-meta");
            boost::archive::text_oarchive ov(metaOfs);
            ov << block_serversMap;
            metaOfs.close();
        }
        else if (type == MetaType::current_id)
        {
            std::ofstream metaOfs("dfsfiles/namenode/current-id-meta");
            metaOfs << to_string(fileID);
            metaOfs.close();
        }
    }
};

class DataServer
{
public:

    explicit DataServer(int _serverId)
    {
        serverId = _serverId;
    }

    void operator() () const
    {
        for (;;)
        {
            unique_lock<mutex> lk(nd_m);
            nd_cv.wait(lk, [this]{return dataNotified[serverId];});

            if (ispropercmd && type == MsgType::put)
            {
                saveFile();
            }
            else if (ispropercmd && type == MsgType::read){

                if (is_ready_read && server_executing_read == serverId)
                {
                    cout << "is ready read and my id is " << serverId << endl;
                    readFileAndOutput();
                }
            }
            cout << serverId << " data notified \n";
            dataNotified[serverId] = false;
            lk.unlock();
            nd_cv.notify_all();
        }

    }

private:
    int serverId;

    void saveFile() const
    {
        // dfs/datanode[1,2,3,4]/
        // check the server_fileRangesMap
        // <serverid, range{from, count, blockid}>
        auto fileRanges = server_fileRangesMap.equal_range(serverId);
        string nodePath = "dfsfiles/datanode" + to_string(serverId);
        for (auto f_it = fileRanges.first; f_it!= fileRanges.second; ++f_it)
        {
            int _blockId = (f_it->second).blockId;
            long long _from = (f_it->second).from;
            int _count = (f_it->second).count;
            char *buffer = new char[block_size_int];
            ifs.seekg(_from, ifs.beg);

            ifs.read(buffer, _count);

            ifs.seekg(0, ifs.beg);

            ofstream myblockFile;

            myblockFile.open(nodePath + "/" + desFileName + "-part" + to_string(_blockId));

            myblockFile.write(buffer, _count);

            myblockFile.close();

            delete[] buffer;
        }


    }

    void readFileAndOutput() const
    {
        string blockpath = "dfsfiles/datanode" + to_string(serverId) + "/"
        + read_logic_file + "-part" + to_string(read_block);

        ifstream blcokFile(blockpath);

        if (blcokFile.is_open())
        {
            char *buffer = new char[block_size_int];
            blcokFile.seekg(offset_in_block, blcokFile.beg);
            blcokFile.read(buffer, read_count);

            cout.write(buffer, read_count);
            cout << endl;

            delete[] buffer;
            blcokFile.close();
        }
        else
        {
            cerr << "Failed to open block" << endl;
        }
    }
};


int main(int argc, char* argv[])
{
    dataserver_num = 4;

    NameServer nameServer;
    thread nsThread(nameServer);

    boost::filesystem::path dfsPath{"dfsfiles"};
    boost::filesystem::create_directory(dfsPath);

    boost::filesystem::path namePath{"dfsfiles/namenode"};
    boost::filesystem::create_directory(namePath);

    for (int i = 0; i < dataserver_num; i++)
    {
        // create four folders
        string nodepath = "dfsfiles/datanode" + to_string(i);
        boost::filesystem::path p{ nodepath};
        boost::filesystem::create_directory(p);

        DataServer ds(i);
        thread dsThread(ds);
        dsThread.detach();
    }

    nsThread.detach();

    string cmd;
    while (getline(cin, cmd))
    {
        ispropercmd = processCmd(cmd);

        {
            lock_guard<mutex> lk(cs_m);
            nameNotified = true;
        }

        cs_cv.notify_all();

        unique_lock<mutex> cs_lk(cs_m);
        cs_cv.wait(cs_lk, []{return processed;});
        cs_lk.unlock();


        server_fileRangesMap.clear();

        if (type == MsgType::put)
        {
            if (ispropercmd)
            {
                cout << "Upload suceesful ! File id = " << fileID - 1 << endl;
            }
            else
            {
                cout << "Failed to upload file" << endl;
            }

            if (ifs.is_open())
            {
                ifs.close();
            }
        }


        processed = false;
    }


}