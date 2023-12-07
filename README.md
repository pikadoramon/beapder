# BEAPDER

![](https://img.shields.io/badge/python-3.6-brightgreen)
![](https://img.shields.io/github/watchers/pikadoramon/beapder?style=social)
![](https://img.shields.io/github/stars/pikadoramon/beapder?style=social)
![](https://img.shields.io/github/forks/pikadoramon/beapder?style=social)

## 简介

1. beapder一开始源于boris-spider基础版本进行开发, 后续随着boris-spider项目推进成feapder。当前beapder重新兼容至feapder 1.8.6版本;
2. beapder是一款适应于当前feapder 1.8.6版本二次开发的亚版本, 主要区别是专注在资源下载处理以及线上生产环境的分布式环境下的应用;
3. beapder尝试融合feapder和scrapy的特性, 因此在某种程度上看。即有点像scrapy又有点像beapder。起初目的是为了让beapder兼容一些scrapy的插件，避免二次开发。逐渐发现与beapder主支版本差异越来越大;
4. beapder是命名源于 better-easy-air-pro-spider 缩写。在实际中考量是牺牲部分速度，来提高稳定性、健壮性以及可维护性。也因此将fast改为better;
5. beapder未来会专注于大文件资源下载采集以及公司生产级别应用场景适配，同时会以scrapy的兼容作为一个支线开发目标继续下去;


读音: `[ˈbiːpdə]`

具体教程还未写

但是可以参考![feapder教程](./RAW_README.md) , 觉得feapder或者beapder能帮到你. 给开发者点个star呗~

feapder author: https://github.com/Boris-code/feapder

beapder contributor: https://github/pikadoramon/beapder

# Beapder适配
新增以下特性
- [x] 增加代理携带校验
- [x] 拉取Apollo日志
- [x] 日志脱敏
- [x] 调整日志实例方式
- [x] 规则爬虫模板
- [x] 定时器线程负责统计运行中数据
- [x] 设置对象导入
- [x] 新增文件导出pipeline

待完成
- [ ] 代理提取
- [ ] 请求上报, 入库上报grafana