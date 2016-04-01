# miniDFS

- [English Version](README_en.md)
- [中文版本](README.md)

来自阿里巴巴云计算课程的实践项目。

## 编译与运行

需要安装`cmake` 3.4及以上版本，和`boost` 1.59.0，要确保编译器支持C++ 11。

建议使用`out-of-source`的编译方式，比如：

```shell
$ mkdir build
$ cd build
$ cmake ..
$ make
```
编译完成后，执行`./DFS`可启动文件系统，然后可以输入miniDFS命令。目前支持的命令：

```shell
# 将本地文件上传到miniDFS；返回ID
$ put source_file_path

# 读取miniDFS上的文件：文件ID，偏移量，长度
$ read file_id offset count
```
