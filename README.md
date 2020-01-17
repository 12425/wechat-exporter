# wechat-exporter

本工具可导出微信文本聊天记录。

使用步骤：

1. 使用 iTunes 备份 iPhone / iPad。Android 用户需将聊天记录迁移到 iOS 设备后备份。

2. 安装 [Python 3](https://www.python.org/downloads/)。

3. 下载[本工具](//github.com/12425/wechat-exporter/archive/master.zip)。

4. 修改配置文件 `conf-wechat-exporter.ini` (见下)。

5. 运行 `wechat-exporter.py`。

  * `root`：iTunes 同步数据所在目录，留空则使用默认目录。
  * `dest`：导出到。
  * `log`：将输出保存到文件。请设置全路径。
  * `compress`：`1` 表示保存为 `.csv.bz2`，`0` 表示保存为 `.csv`。
  * `bom`：`1` 表示保存为 `UTF-8 with BOM` 以兼容 MS Excel，`0` 表示保存为 `UTF-8`。

  一个示例配置文件：

``` ini
[DEFAULT]
dest=~/wechat-logs
log=~/wechat-logs/wechat-exporter.log
compress=1
bom=0
```

测试环境
* iPhone 4S (iOS 9.3.5)
* iPhone 6 (iOS 11.4)

参考
* [python backup tools for IOS5+](https://github.com/bo01ean/iphone-tools "python backup tools for IOS5+ - GitHub")
* [iOS 微信的本地存储结构简析](https://zhuanlan.zhihu.com/p/22474033 "iOS 微信的本地存储结构简析 - 伪红学家的文章 - 知乎专栏")
