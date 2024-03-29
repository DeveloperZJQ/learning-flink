# 电商用户行为分析

## 统计分析

    - 点击、浏览
    - 热门商品、近期热门商品、分类热门商品，流量统计

## 偏好统计

    - 收藏、喜欢、评分、打标签
    - 用户画像、推荐列表（结合特征工程和机器学习算法）

## 风向控制

    - 下订单、支付、登陆
    - 刷单监控、订单失效监控、恶意登陆（短时间内频繁登陆失败）监控

## 指标

### 实时统计分析

    - 实时热门商品统计
    - 实时热门页面流量统计
    - 实时访问流量统计
    - APP市场推广统计
    - 页面广告点击量统计

### 业务流程及风险控制

    - 页面广告黑名单过滤
    - 恶意登陆监控
    - 订单支付失效监控
    - 支付实时对账

## 项目模块

### 实时热门商品统计

    基本需求
    - 统计近一个小时内的热门商品，每5分钟更新一次
    - 热门度用浏览次数（pv）来衡量
    解决思路
    - 在所有用户行为数据中，过滤出浏览（pv）行为进行统计
    - 构建滑动窗口，窗口长度为1小时，滑动距离为5分钟