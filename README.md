# wechat-exporter

本工具可导出微信文本聊天记录。

使用步骤：
1. 安装 [Python 3](https://www.python.org/downloads/)。
2. 下载本工具。
3. 修改配置文件 `conf-wechat-exporter.ini`。
  * root：iTunes 同步数据所在目录，留空则使用默认目录。
  * dest：导出到。
  * log：将输出保存到文件。请设置全路径。
  * compress：1 表示保存为 .csv.bz2，0 表示保存为 .csv。

