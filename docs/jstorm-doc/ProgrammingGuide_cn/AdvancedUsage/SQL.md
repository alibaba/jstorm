---
title:  "How to run SQL on jstorm"
layout: plain_cn

#sub-nav-parent: AdvancedUsage
sub-nav-group: AdvancedUsage_cn
sub-nav-id: SQL_cn
sub-nav-pos: 7
sub-nav-title: SQL-on-JStorm
---

* This will be replaced by the TOC
{:toc}

# 1. SQL规范

## 2.1. DDL语句（流定义）
    1、支持普通事件流的定义和复杂事件流的定义。
    2、支持字段类型int,long,number,boolean,String,date,Map(仅对象流支持) 
	
### 2.1.1. 普通流事件定义
    CREATE STREAM stream_def
    (
        col_name data_type not null,
        col_name data_type  [PRIMARY KEY(col_name, ...)] ,
        ...
    )
    [
        WITH
        (
            ...
        )
    ]
	
### 2.1.2. 复杂结构事件流的定义
    create OBJECT STREAM stream_def
    (
        col_name:"date_type ",
        ...
    )
    [
        WITH
        (
            [primaryKey='col_name',]
            [notNull='col_name,...',]
            ...
        )
    ]

## 2.2. DML语句（指标定义）

### 2.2.1. 窗口
    1、时间窗口time(timeType, length, size)
        timeType 一般取值’natural’
        length 窗口总长度，取值范围: 数字(s|m|d)
        size 窗口大小，取值范围: 数字(s|m|d)
        举例:time('natural', '3d', '5m') length必须大于size。

    2、时间窗口time(timeType, length, size,timefield)
        timeType 一般取值natural
        length 窗口总长度，取值范围: 数字(s|m|d)
        size 窗口大小，取值范围: 数字(s|m|d)
        timefield 时间字段，指定窗口的时间字段
        举例:time('natural', '3d', '5m','regtime') length必须大于size。

### 2.2.2. 指标定义
    insert into 指标名 select select_list
    from stream_def.time() [as name] [, stream_def.time() [as name]] [,...]
    [where search_conditions]
    [group by grouping_expression_list]
    [having grouping_search_conditions]
    [order by order_by_expression_list]
    [limit num_rows]

#### 2.2.2.1. 聚合查询
支持聚合算法: count,sum,max,min,avg,stddev,first,last

    insert into 指标名
    select count() as name [,sum() [as name]] [,avg() [as name]]  [,max() [as name]]
    from stream_def.time()
    where [where search_conditions]
    [group by grouping_expression_list]
    举例：
    insert into trade_hits select count(0) as a ,sum(fee) as b
    from trade.time('natural', '3d', '1d') 
    where fee < 100 group by buyerId

#### 2.2.2.2. 去重聚合查询
支持的聚合算法:count,sum,avg

    去重算法类型: 
    single(单个),
    all(所有),
    S(按自然秒),
    M(按自然分),
    H(按自然小时),
    D(自然天),
    W(按自然周),
    O(按自然月),
    Y(按自然年)。

    insert into 指标名
    select count(DISTINCT) as name [,sum() [as name]] [,avg() [as name]]  [,max() [as name]]
    from stream_def.time()
    where [where search_conditions]
    [group by grouping_expression_list]

    举例：
    insert into trade_hits 
    select count(DISTINCT userId,1,'single') as a ,sum(DISTINCT orderId, fee, 'single') as b 
    from trade.time('natural', '3d', '1d') 
    where fee < 100 group by buyerId

#### 2.2.2.3. top排名
支持聚合算法: count,sum,max,min,avg,stddev

    insert into 指标名
    select count() as name [,sum() [as name]] [,avg() [as name]]  [,max() [as name]]
    from stream_def.time()
    where [where search_conditions]
    group by grouping_expression_list
    [having grouping_search_conditions]
    order by order_by_expression_list
    limit num_rows

    举例：
    insert into trade_user_item_fee 
    select sum(fee) as fee from trade.time('natural', '3d', '1d')
    where fee < 100 group by userId,buyid 
    order by fee limit 100;

#### 2.2.2.4. 多流合并
    insert into 指标
    select count() as name [,sum() [as name]] [,avg() [as name]]  [,max() [as name]] from (
        select select_list from stream_def [where search_conditions]
        union
        select select_list from stream_def [where search_conditions]
    ).time() as name
    where [where search_conditions]
    group by grouping_expression_list
    [having grouping_search_conditions]
    order by order_by_expression_list
    limit num_rows

    举例:
    insert into 指标
    select count(userid) from (
        select memid as userid from login.time() where a.x1= ‘abc’
        union
        select userid as userid form register_web.time() where b.x= ‘def’
    )as login.time()

#### 2.2.2.5. 子查询
    通过指标引用(INDICATOR函数)查询的方式来支持子查询：
    insert into table indi_code
    SELECT COUNT(1) AS exception_ip_user_num_24h_tooMany
    FROM Register_web.time('natural', '3d', '5m')
    WHERE INDICATOR('base_reg_ip_user_num_1h', '1h', ip) >= 4

#### 2.2.2.6. 指标定义规范
（1）计算约束

    1、时间窗口字段必须是long型；
    2、字段类型必须与对应操作匹配；
    3､ 一条sql目前最多支持4个指标的计算；
    4､ 一条sql中仅且只能包含一个去重指标；
    5､ 去重算法仅支持cout,sum；
    6､ group by 控制在3个字段范围内；
    7、group by 中仅且允许出现一个值是数组的字段；
    8、支持limit,limit取值<=1000。

（2）窗口(时间片)约束

    支持: 秒(s), 分(m), 小时(h), 天(d), 周(w), 月(o), 年(Y)

（3）生命周期约束

    窗口长度(时间片长度)=生命周期/窗口大小
    如：生命期3d,窗口大小1h，窗口长度=3d/1h=72
    1､ 普通指标1000以内的窗口长度；
    2､ 连续去重指标120以内的窗口长度；
    3､ 最长生命周期支持7天；
    4､ 当两者出现冲突时取最小约束。

（4）场景约束

    1､ 需要补全的计算，需要在数据接入NUT前完成补全；
    2､ 目前不支持计算过程中纬表关联或子查询；
    3､ 对于生命周期长(>2d)的指标，如果要预热历史数据，必须先离线计算，然后再按约束导入到实时计算进行指标预热。(去重指标预热因在线、离线去重方式不一样会造预热偏差)；
    4､ 对计算结果实时性不敏感(>7d)的数据建议走离线；
    5、不支持计算过程中需要对计算结果做二次加工。例如: select sum(totalfee)-sum(fee) from trade;
    6、目前暂不支持join；
    7､ 不支持流套接;
    8､ 支持计算准确性99.999%。

## 2.3.	内建函数
**Substring函数**

    功能：
        截取字符串(参考mysql使用)

    使用：
        substring(str, pos) 截取str从pos位置开始及之后的字符串
        substring(str, pos, length) 截取str从pos位置开始，长度为length的字符串

    参数说明：
        str: 被截取字段
        pos: 从第几位开始截取，pos从1开始
        length: 截取长度
        返回值：字符串

    举例:
        SELECT substring('2015-07-14 01:02:03',12);         /* '01:02:03' */
        SELECT substring('2015-07-14 01:02:03',12,8);       /* '01:02:03' */

**Concat函数**

    功能：
        该函数用于连接某两个属性的值, 两个属性值通过特定的自定义连接符连接, 主要用于情报中心城市关系的top指标当中

    使用：
        concat(first,second) 将first与second使用连接符"__"进行连接并返回
        concat(first,second,third) 当third不为空时，将first、second与third使用连接符"__"进行连接并返回，否则，将first与second使用连接符"__"进行连接并返回

    适用范围: 
        groupby语句体, orderby语句体

    返回值类型: 
        字符串

    举例:
        SELECT concat('first','second');  /* fisrt__second */
        SELECT concat('first','second','third'); /* first__second__third */
        select count(Get(extras.itemId))  over(order by CONCAT(Get(extras.brandName),Get(extras.sellerCity),”\__”) as extra_ssic_brand_city_itemNum from ssic.time('natural', '3d', '1d') GROUP BY \_biz_Category,   CONCAT(Get(extras.brandName),Get(extras.sellerCity),'__')

**Length函数**

    功能：
        返回字段的字符串长度，如果字段为整数，则先将其转为字符串，再返回其长度

    使用：
        length(column) 返回字符串长度

    返回值： 
        整数
    
    举例：
        SELECT length('abc');   /* 3 */
        SELECT length(123456);  /* 6 */
        SELECT length(column);

**RandomFunc函数**

    功能：
        返回一个随机整数；
   
    使用：
        RandomFunc() 返回[0,10)之间的随机整数
        RandomFunc(num) 返回[0,num)之间的随机整数
    
    返回值：
        整数

    举例：
        SELECT RandomFunc();  /*返回[0,50)之间的随机数*/
        SELECT RandomFunc(num); /*返回[0,num)之间的随机数*/

**DateFormat函数**

    功能：
        日期格式化

    使用：
        DateFormat(date,format) 将日期根据指定的格式进行格式化

    返回值：
        字符串

    举例：
        SELECT DateFormat(now(),'yyyy-MM-dd HH:mm:ss'); /*  2015-07-14 09:56:51  */

**To_Char函数**

    功能：
        转字段值转换为字符串

    使用：
        To_Char(column)

    返回值：
        字符串

    举例：
        SELECT To_Char("abc");  /* 'abc' */
        SELECT To_Char(123);  /* '123' */
        SELECT To_Char(true); /* 'TRUE' */

**To_Number函数**

    功能：
        将字段值转为整数

    使用：
        To_Number(column)

    返回值： 
        整数

    举例：
        SELECT To_Number("123");  /* 123 */
        SELECT To_Number(123);  /* 123 */

**Biz_Div函数**

    功能：
        除法

    使用：
        Biz_Div(numerator, denominator)

    参数：
        numerator 分子、被除数
        denominator 分母 、 除数

    返回值：
        整数

    举例：
        SELECT Biz_Div(10,5);  /* 2 */
        SELECT Biz_Div('10','5'); /* 2 */
        SELECT Biz_Div('10',4); /* 2 */

**If_Within_Time_Range函数**

    功能：
        判断时间点是否落在时间区间内

    使用：
        If_Within_Time_Range(startTime,endTime) 判断当前时间是否在区间(startTime,endTime)之间
        If_Within_Time_Range(startTime,endTime,time) 判断时间time是否在区间(startTime,endTime)之间

    参数：
        startTime 时间区间的起始时间
        endTime 时间区间的终止时间
        time 需要判断的时间
        所有参数均为字符串类型，格式为yyyy-MM-dd HH:mm:ss

    返回值：
        布尔类型

    举例：
        SELECT If_Within_Time_Range('2000-01-01 00:00:00','2014-12-31 23:59:59');  /* false */
        SELECT If_Within_Time_Range('2000-01-01 00:00:00','2014-12-31 23:59:59','2008-08-08 08:08:08');  /* true */

**Match函数**

    功能：
        该函数用于实现类似sql中like的功能

    使用：
        Match(matchPattern, column) 判断字段值是否匹配模式matchPattern

    参数：
        matchPattern: 匹配模式
        column： 需要进行匹配的字段

    返回值：
        布尔类型

    举例：
        SELECT match('%def%','abcdefgh'); /* true */
        SELECT match('abc%','abcdefgh'); /* true */
        SELECT match('%gh','abcdefgh'); /* true */
        SELECT match('%abc%','abc');    /* true */
        SELECT match('%abc%','bcd');    /* false */
        SELECT COUNT(DISTINCT extras.sellerId, 1, 'all') AS extra_ssic_intellectural_sellerNum FROM ssic.time('natural', '3d', '5m')  WHERE Match( '%111111%' ,GET(extras.sellerId)) .

**Like_Strings_Separated_By_Crlf函数**

    功能：
        判断字符串是否包含一组字符串的任意一个

    使用:
        Like_Strings_Separated_By_Crlf(srcStr,likeStr)

    参数：
        srcStr 需要判断的字符串
        likeStr 由分隔符\n组成的模式字符串

    返回值：
        布尔类型

    举例：
        SELECT Like_Strings_Separated_By_Crlf('aabbccddee','aaa\nbbbb\ncccc\nd'); /* true */
        SELECT Like_Strings_Separated_By_Crlf('aabbccddee','f\ng\nh\ni'); /* false */

**IsTimeBetweenN函数**

    功能：
        判断时间是否距离当前时间N天以内

    使用：
        IsTimeBetweenN(date, n)

    参数：
        date 需要进行判断的时间，类型为Date日期类型
        n 数值，表示n天

    返回值：
        布尔类型

    举例：
        SELECT IsTimeBetweenN(now(),10); /* true */

**Indicator函数**

    功能： 
        指标查询函数

    使用：
        Indicator(indicatorCode,tWindow,key) 获取指标indicatorCode在最近窗口时间tWindow内key对应的字段值

    参数：
        indicatorCode 指标名称
        tWindow 时间窗口
        key 字段名称

    举例：
        SELECT COUNT(1) AS exception_ip_user_num_24h_tooMany
        FROM Register_web.time('natural', '3d', '5m')
        WHERE INDICATOR('base_reg_ip_user_num_1h', '1h', 'ip') >= 4
        /* 查询指标base_reg_ip_user_num_1h中,最近1小时,对应ip的值。*/


**Bit_Wise_And函数**

    功能：
        测试位与功能

    用法：
        Bit_Wise_And(num1,num2) 当num1、num2同时为数值，num1>0 、num2 > 0 、(num1 位与 num2)=num2同时成立时，结果为true,否则为false

    返回值：
        布尔类型

    举例：
        SELECT Bit_Wise_And(255,15); /* true, 因为255>0 , 15>0 , 255 & 15 = 15 */

**Bit_Wise_And_Any函数**

    功能：
        测试位与功能

    用法：
        Bit_Wise_And_Any(num1,num2)
        num2以","为分隔符的字符串
        当num1为数值、num1>0 、num2分割之后的数字其中有一个或以上(设为n)满足n>0 、(num1 位与 n)=n时，结果为true,否则为false

    返回值：
        布尔类型

    举例：
        SELECT Bit_Wise_And_Any(255,"0,15"); /* true, 因为255>0 , 15>0 , 255 & 15 = 15 */
        Str_Array_Equals函数

    功能：
        测试源字符串组中是否有一个与目标字符串组相同

    使用：
        Str_Array_Equals(srcString, matchString)

    参数：
        srcString 以'\n'为分隔符组成的字符串
        matchString 以'\n'为分隔符组成的字符串

    返回值：
        布尔类型

    举例:
        SELECT Str_Array_Equals('aa\nbb\ncc','aa\ncc');  /*源中的aa=目标中的aa,所以结果为true*/
        SELECT Str_Array_Equals('aa\nbb\ncc','dd\nff');  /*源中的aa、bb、cc都与dd、ff不同，所以结果为false*/

## 2.4.	用户自定义函数

系统支持通过自定义函数处理事件的内容。具体定义约束可参照下面的模板。

自定义函数模板：

```
    import com.alibaba.maat.epl.function.UDTF;
    import com.alibaba.maat.epl.stream.DataType;
    import com.alibaba.maat.epl.value.Value;
 
    //两个日志相关的可选包
    import org.slf4j.Logger;
    import org.slf4j.LoggerFactory;
 
    /*导入函数需要依赖的类库*/
 
    /*
　   * 1、${ClassName}　是关键字,勿做任何修改,否则会影响编译!!!!
　   * 2、默认继承UDF,勿做任何修改,否则影响程序编译执行!!!!!!
　   */
    public class $className extends UDFFunction {
 
        /**
         * 初始化UDF函数参数类型(目前系统支持的类型        DataType.INT,DataType.STRING,DataType.NUMBER,DataType.DATE,DataType.OBJECT)
         */
        @Override
        public DataType[] initialize() {
            // example:
            // DataType[] paramTypes = new DataType[3];
            // paramTypes[0] = DataType.STRING; 参数1为字符串型
            // paramTypes[1] = DataType.INT; 参数2为int型
            // paramTypes[2] = DataType.INT; 参数3为init型
            // return paramTypes
        }
 
        /**
         * 方法执行体(类型odps中UDF函数中的process方法)
         */
        @Override
        public Object call(Object[] params) {
            // example:
            // 参数数组中 params[0]对应参数1, params[1]对应参数2, params[2]对应参数3
            //调用print函数可以将打印日志到maat/logs/udtf.log文件中 方便调试!!!!
            //print("打印日志到maat/logs/udtf.log文件中 !!!!");
        }
 
        /**
         * 此方法用来验证参的值，如果有验证参数值的需求可以在此方法写业务逻辑
         */
        @Override
        public void validateParam(Value[] values) {
            // TODO Auto-generated method stub
        }
 
        /**
         * 设计返回值的类型
         */
        @Override
        public Class<?> getReturnClass() {
            // example:返回double型数据
            // return Double.class;
     
        }
```

## 2.5. 双流Join
暂未提供该功能


