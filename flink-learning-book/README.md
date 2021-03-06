# 学习参照书籍-《Flink原理、实战与性能优化》
## CH1 Apache Flink介绍
### 1.1 Apache Flink是什么？
### 1.2 数据架构的演变
#### 1.2.1 传统数据基础架构
#### 1.2.2 大数据数据架构
#### 1.2.3 有状态流计算架构
#### 1.2.4 为什么会是Flink
### 1.3 Flink应用场景
### 1.4 Flink基本架构
#### 1.4.1 基本组件栈
#### 1.4.2 基本架构图
### 1.5 本章小结
## CH2 环境准备
### 2.1 运行环境介绍
### 2.2 Flink项目模板
#### 2.2.1 基于Java实现的项目模板
#### 2.2.2 基于Scala实现的项目模板
### 2.3 Flink开发环境配置
#### 2.3.1 下载IntelliJ IDEA IDE
#### 2.3.2 安装Scala Plugins
#### 2.3.3 导入Flink应用代码
#### 2.3.4 项目配置
### 2.4 运行Scala REPL
#### 2.4.1 环境支持
#### 2.4.2 运行程序
### 2.5 Flink源码编译
### 2.6 本章小结
## CH3 Flink编程模型
### 3.1 数据集类型
### 3.2 Flink编程接口
### 3.3 Flink程序结构
### 3.4 Flink数据类型
#### 3.4.1 数据类型支持
#### 3.4.2 TypeInformation信息获取
### 3.5 本章小结
## CH4 DataStream API介绍与使用
### 4.1 DataStream编程模型
#### 4.1.1 DataSources数据输入
#### 4.1.2 DataStream转换操作
#### 4.1.3 DataSink数据输出
### 4.2 时间概念与watermark
#### 4.2.1 时间概念类型
#### 4.2.2 EventTime和watermark
### 4.3 windows窗口计算
#### 4.3.1 windows Assigner
#### 4.3.2 Windows Function
#### 4.3.3 Trigger窗口触发器
#### 4.3.4 Evictors数据剔除器
#### 4.3.5 延迟数据处理
#### 4.3.6 连续窗口计算
#### 4.3.7 Windows多流合并
### 4.4 作业链和资源组
#### 4.4.1 作业链
#### 4.4.2 Slots资源组
### 4.5 Asynchronous I/O异步操作
### 4.6 本章小结