
## 大数据学习历程 ##

### 一：杂例 ###

- wordcount(spark 版本)
- 利用spark分布式计算Pi的值
- simplescala，对于scala语言的学习
- ScalaExample，对于一下算子在此测试
### 二：电影受众分析（spark core） ###
**目标**：

- 用户年龄性别分布
- 最受欢迎的电影
- top电影（得分最高的电影）
### 三：电影受众分析（spark sql） ###
同上，只是用spark sql重新实现了一遍
### 四：慕课网日志分析实战 ###
#### firstETL ####
清洗可以分为多个步骤，第一步是拿到我们想要的数据
主要采用map,filter算子将得到的脏数据进行清洗。得到我们想要的数据

**目标：**

- 按照流量进行统计TopN的课程
- 按照地市进行统计
- 最受欢迎的TOPN课程
- 最后将统计好的数据插入到数据库中，进行结果的可视化

### 五：用户点击行为实时分析系统(待更新) ###
采用技术点kafka + sparkstreaming
模拟用户的点击行为，实时的分析用户的行为
### 六：twitter情感分析（待更新） ###
采用的技术点sparkstreaming + mysql
twitter开放了实时的信息流，通过实时的获取流信息得到最新的热点事件，最新的热点人物等。

### 七：练习使用git
练习使用branch
怎么没有出现处理冲突的代码呢？
为什么没有处理冲突的代码呢？





