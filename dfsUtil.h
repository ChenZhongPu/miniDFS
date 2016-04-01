//
// Created by ChenZhongPu on 3/31/16.
//

#ifndef MINIDFS_DFSUTIL_H
#define MINIDFS_DFSUTIL_H
#include <sstream>
#include <string>
#include <vector>
#include <utility>
#include <iostream>

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

struct TreeNode
{
    std::string value;
    bool isFile;
    TreeNode* parent;
    TreeNode* firstChild;
    TreeNode* nextSibling;
    TreeNode(std::string _value, bool _isFile): value(_value), isFile(_isFile), parent(nullptr), firstChild(nullptr), nextSibling(
            nullptr) {}
};

class FileTree
{
public:
    TreeNode *root;

    bool findNode(const std::string value, bool isFile, TreeNode* node_parent)
    {
        std::vector<std::string> files = split(value, '/');

        TreeNode* node = root->firstChild;

        bool isFound = true;


        for (int i = 0; i < files.size(); i++)
        {
            std::string file = files[i];

            bool _isFile = isFile;
            if (i < files.size() - 1)
            {
                _isFile = false;
            }

            while (node != nullptr && node->isFile != _isFile && !file.compare(node->value))
            {
                node = node->nextSibling;
            }

            // if temp is nullptr, meaning that cannot find such node
            if (node == nullptr)
            {
                isFound = false;
                break;
            }
            else if (node->isFile == _isFile && file.compare(node->value))
            {
                node_parent = node;
                node = node->firstChild;
            }
            else
            {
                isFound = false;
                break;
            }
        }
        delete node;
        return isFound;
    }

public:
    FileTree()
    {
        root = new TreeNode("/", false);
    }

    void insertNode(const std::string value, bool isFile)
    {
        TreeNode *nodeParent = root;
        bool isFound = findNode(value, isFile, nodeParent);

        if (!isFound)
        {
            std::vector<std::string> files = split(value, '/');
            TreeNode* newNode = new TreeNode(files.back(), isFile);

            newNode->parent = nodeParent;

            TreeNode *temp = nodeParent->firstChild;

            if (temp == nullptr)
            {

                nodeParent->firstChild = newNode;
            }
            else
            {
                while (temp->nextSibling != nullptr)
                {
                    temp = temp->nextSibling;
                }
                temp->nextSibling = newNode;
            }
        }
    }


    void printall(TreeNode * node)
    {
        if (node != nullptr)
        {
            std::cout << node->value << std::endl;
            printall(node->nextSibling);
            printall(node->firstChild);
        }
    }

    void printall()
    {
        printall(root->firstChild);
    }

};

#endif //MINIDFS_DFSUTIL_H
