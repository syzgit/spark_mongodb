package bayes_charts
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.ml.feature.IDF
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import java.lang.String
import scala.collection.mutable
//import org.apache.commons.lang.mutable.Mutable
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.DriverManager
import java.sql.Statement
import org.apache.log4j.{Level, Logger}
import java.io.PrintWriter
import java.io.File
object fenlei {
  def main(args:Array[String]){
     Logger.getLogger("org").setLevel(Level.ERROR)
     val conf = new SparkConf().setAppName("YZSUN")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
     import sqlContext.implicits._
    //取要分类的数据 mongo_hb_tables_jiexi_data
   var RDD_bson = sc
   .textFile("/user/yzsun/10-month/juchao_jiexi_data/_new_alldata_/*")
  //.textFile("/user/yzsun/result_juchao_alldata/")
   //.textFile("/user/yzsun/mongo_hb_tables_jiexi_data/")
  //.textFile("C:/Users/yzsun.abcft/Desktop/juchao_4_test.txt")
  //.map(row=> row.replace(",", "").split(" "))
  .map(row=> row.substring(4, row.length()-1).replace("-", "").replace(">", "").split(" "))
   //RDD_bson.foreach { x => x.foreach(print) }
   //取模型  
  /*var RDD_zichan = sc
  .textFile("/user/yzsun/bayes_train_result/991.txt").map(x => x.split(" "))
  var RDD_xianjin = sc
  .textFile("/user/yzsun/bayes_train_result/992.txt").map(x => x.split(" "))
  var RDD_lirun = sc
  .textFile("/user/yzsun/bayes_train_result/993.txt").map(x => x.split(" "))*/
   var RDD_zhuying = sc
  .textFile("/user/yzsun/bayes_train_result/997-putong-10.12.txt").map(x => x.split(" "))
  
 //999_8.txt是银行类靠谱的,997-putong-10.12.txt是普通靠谱的，998.txt是证券
  
  /*var RDD_zichan = sc
  .textFile("C:/Users/yzsun.abcft/Desktop/three_baobiao/991.txt").map(x => x.split(" "))
  var RDD_xianjin = sc
  .textFile("C:/Users/yzsun.abcft/Desktop/three_baobiao/992.txt").map(x => x.split(" "))
  var RDD_lirun = sc
  .textFile("C:/Users/yzsun.abcft/Desktop/three_baobiao/993.txt").map(x => x.split(" ")) 
  var RDD_zhuying = sc
  .textFile("C:/Users/yzsun.abcft/Desktop/three_baobiao/996.txt").map(x => x.split(" "))*/
  
 
       //将分类数据转换成id -> 数据形式
   val bson_rdd_buffer =  RDD_bson.map { 
   var m2=Map[String,ArrayBuffer[String]]()
     x => {
         var m1=Map[String,ArrayBuffer[String]]()
         val bson_buffer = ArrayBuffer[String]()
         for (i <- 0 until x.length){
          val bson0 = x(1)
          if (i !=1 && x(i).length()>=2){
              bson_buffer += x(i)
           }
          m1 += (bson0 -> bson_buffer)
         }
         m1.foreach{
               case(key,value)=>
                 {
                   m2 += (key -> value)  
                 }
             }
         m1
       }
     }
     
     //将资产负债表模型转换成数组,因为后面要跟分类数据做匹配，2个RDD不能做for循环，因此将模型数据转换成数组
   /*val zichan_rdd_arr =  RDD_zichan.toArray()(0)
   val xianjin_rdd_arr=  RDD_xianjin.toArray()(0)
   val lirun_rdd_arr =  RDD_lirun.toArray()(0)*/
 
   val zhuying_rdd_arr =  RDD_zhuying.toArray()(0)
   
   //得到分类结果
  /* val j_zichan =    model_classfy(bson_rdd_buffer,zichan_rdd_arr,1)
   val j_lirun =    model_classfy(bson_rdd_buffer,lirun_rdd_arr,2)
   val j_xianjin =    model_classfy(bson_rdd_buffer,xianjin_rdd_arr,3)*/
   val j_zhuying =    model_classfy2(bson_rdd_buffer,zhuying_rdd_arr,4)
 
   //j_zichan.foreach(println)
   
  //合并结果去重
   // val san1 = j_xianjin.union(j_zichan).union(j_lirun).union(j_zhuying)
       //val san1 = j_yingli.union(j_yingyun).union(j_chengzhang).union(j_zhuying)

    val san2 = j_zhuying.distinct()
   
 
    /*j_zhuying.foreach{
     println
             }*/
    //发现不repartition的时候生产8万多个文件，在后面进行计算的时候又会生成8万多个文件，这样在生成文件的时候花费大量时间，
    //因此试试只生产1000个文件的话对后面的有没有优化作用
    san2.repartition(1).saveAsTextFile("/user/yzsun/11-month/juchao_fenlei_result/_new_alldata_putong_")
    
  sc.stop()
  }
  
  
  def model_classfy(bson_rdd_buffer:RDD[Map[String, ArrayBuffer[String]]],xianjin_rdd_arr: Array[String],tag:Int):RDD[Map[String, String]]={
     var xianjin_map2=Map[String,String]()
      val j_xianjin =      bson_rdd_buffer.map{
      x => 
        {
          var xianjin_map1 :Map[String,String] = Map()
          x.foreach{
          case(key,value)=>{
             var k =0
             var companymapfeatures:Map[String,String] = Map()
              var xianjin_map :Map[String,String] = Map()
               val xianjin_length = (value.length).toDouble
                   for (j <- 0 until value.length) 
                  {  
                  for (i <- 0 until xianjin_rdd_arr.length){
                    if (value(j).equals(xianjin_rdd_arr(i)))
                      {
                        k = k+1
                      }
                                                            }          
                                                          }
             val kmore = k.toDouble / xianjin_length
                   // if (k>3 ){
              if (kmore > 0.93 && k>20 && xianjin_length < 55 && tag==1){
                         
                     xianjin_map += (key ->"资产负债表")
                 
                           }
             // if (k>3 ){
              if (kmore > 0.88 && k>15 && xianjin_length < 50 && tag==2 ){
                         
                     xianjin_map += (key ->"利润表")
                 
                           }
              // if (k>3){
               if (kmore > 0.85 && k>15 && xianjin_length < 50 && tag==3 ){
                         
                     xianjin_map += (key ->"现金流量表")
                 
                           }
                   /* else
                 {
                     xianjin_map += (key ->"")
                 }*/
                 xianjin_map.foreach
                   {
                     case (key ,value)=>
                   {
                       xianjin_map1 += (key -> value)
                    }
                       }
   } 
                }
              xianjin_map1
        }
    }
     j_xianjin
  }
  
  //一般情况下的分类
  def model_classfy2(bson_rdd_buffer:RDD[Map[String, ArrayBuffer[String]]],xianjin_rdd_arr: Array[String],tag:Int):RDD[Map[String, String]]={
     var xianjin_map2=Map[String,String]()
     val arr_del=Array("本年数","增长率","及以后","入比上年同期","占总营业毛利比例","年月日","毛利率比上年增","比上年同期","上期金额", "、主营业务", "上年同期发生额", "主营业务分行业、分产品情况的说明",  "比上年同期", "年季度","占总营业毛利比例", "前五大国内工程物流项目", "其他国内工程物流项目", "国内工程物流业务合计", "主营业务合计",   "营业支出合计", "其他综合收益", "可供出售金融资产公允价值变动", "综合收益总额","/年度","年度实际数","年月发生额", "年度发生额","年月实际数", "年月预测数",
         "总体毛利率", "、主营业务成本","、其他业务成本","少个百分点","类型","、主营业务收入及成本","截至月十日止年度", "截至八月十日止五個月", "零年", "零年", "零五年","国外占比", "国内占比", "区域名称年","本期发", "生额", "业成本", "收入比上年同期增减", "成本比上年同期增减","比上年同期增减百分点","营业收入比上年同期", "营业成本比上年同期",  "公司",  "平均值", "中值", "医药公司","天原集团", "鸿达兴业", "中泰化学", "内蒙君正", "英力特", "平均值", "天伟化工", "毛利润", "产品或", "手机搜索用户万", "主营业务合计","减少个百分", "行业平均", 
         "营业收入比上年增减","分类","项目名称",  "营业毛利", "营业收入营业成本营业毛利毛利率","比例", "同比增", "营业毛利率比上年同期增减","证券代码", "证券简称", "首发上市日期", "年月/", "年度/", "SH", "亨通光电", "存货", "中天科技", "SZ", "通鼎光电", "通光线缆", "金信诺", "发行人","、营业总收入", "、营业总成本", "、营业利润", "、利润总额", "五、净利润", "年金额万元","品牌类型", "球型支座元/座", "单位售价毛利率",  "期间","上年同","本期发生额上年同期数","公司", "直营", "朗姿股份", "维格娜丝", "欣贺股份",  "年已审实现数","标标标标的的的的公公公公司司司司", "项项项项目目目目", "年年年年月月月月","年年年年", "亚新科凸轮轴", "营业收入增长率净利润净利润增长率", "亚新科双环", "亚新科", "营业收入增长率", "仪征铸", "净利润增长率", "亚新科山西", "亚新科NVH", "GACGI", 
         "减少个百分点","利率","业态","增加个百分点","项目名称","月重列","同比增减","综合毛利率","项目", "毛利率同", "比增长", "销售毛利率","占营业成本比重",  "营业利润","毛利额", "年实际数", "本年发生额", "上年发生额",  "月实际数", "年预测数月预计数", "年合计", "年预测数", "其中收购方", "被收购方", "年实际数", "月实际数", "公司前五名客户的营业收入情况", "期间", "前五名客户营业收入合计", "占营业收入的比例",   "分行业或分产品营","无。",  "分门店类型", "营业收入比上年营同期增减", "分行业或分产品",  "其它行业", "主营业务分产品情况", "收入占比","主营业务", "安徽省内", "安徽省外", "建筑面积万平方米","项目","分行业或分产品", "经营面积万平方米", "主营业务分产品情况", "营业成本比上年增减",  "减少个百分点", "分行业/",  "B、B、C、C、C", "合并", "营业收入比上", "增加个百分", "营业收入比上",  "总体",   "营业成本", "毛利率", "营业收入比上年同期增减", "营业成本比上年同期增减", "毛利率比上年同期增减",  "营业成本比上毛利率比上年年同期增减同期增减", "或分产品", "其他业务", "营业毛利",  "、上海地区", "、除上海以外的华东地区","租金收入", "度金额之比", "年度同比增长",  "毛利营业收入营业成本率比上年增减", "、主营业务","分行业/产品", "人民币千元",   "营业成本比毛利率比上年上年同期增减同期增减","产品名称营业收入元营业成本元营业利润率","月重列", "营业利润",  "毛利率比上年同期增减幅度较大的原因说明","分行业或分产品营业收入营业成本毛利率", "分行业/分产品",  "本期", "分行业营业收入营业成本毛利率","业成本上年增减","毛利率年同期", "毛利率营业收入比营业成本上年增减","主营业务成本比上年增减", "年月份", "占主营业务收入比重", "毛利营业收入比营业成本率上年增减",  "营业收入比营业成本毛利上年增减","主营业务按地区列示如下", "调整前口径", 
         "调整后口径", "营业成本毛利率营业收入比上年增","营业支出",  "收入万元", "成本万元","比上年","同比增幅", "同比增减", "上期发生额","序号", "项目全国","分行业或分产品", "客运量亿人", "旅客周转量亿人公里", "项目江西省",  "合计/综合毛利率","毛利率比上年增比上年增减减","营业成本毛利率","毛利率贡献率","年度上年决算数", "本年金额", "上年金额","市场供给", "图书各类万种", "图书总印数亿册/张", "图书定价总金额亿元", "市场需求", "全国新华书店系统、出版社自办发行单位纯销售数册/张/份/盒", "全国新华书店系统、出版社自办发行单位纯销售金额亿元", "营业收入比上年同期增","营业成本比上年同期增","本期发生数", "上期发生数","年收入万元占比","业务分类","年毛利万元","年E", "月的占比", "年月占比", "年占比",
         "营业支出比上年增减",  "分支机构", "公司总部及境内子公司",  "抵销",    "国外销售","千港元", "千港元經審核","毛利率比上年同期增","种类","毛利率的变化","行业本期比本期数上年增减","本期比上本期数年增减本期数本期比上年增减","主营收入占比", "年月交易前","年月交易后", "单位售价", "元/座","主要资产", "重大变化说明", "股权资产", "固定资产", "无形资产", "同比减少", "在建工程", "应收票据", "年内到期的非流动资产", "长期应收款", "开发支出", "长期待摊费用", "递延所得税资产", "其他非流动资产",
          "主营业成本元", "坪效万元/m", "年月", "营业收入营业成本毛利率比上年增减",  "营业收入营业成本毛利率", "其它地区", "总计",   "年同期增减",  "rrayBuffer(单位：","个百份点",  "抵销","加工中心",  "总部及其他", "上升百分点","其他国家和地区,", "新增", "下降百分点", "主营业务主要产品情况",   "毛利率营业收入比上年同期增减","上年同期增减", "营业收入营业成本毛利率营业收入比上年同期增减",  "分行业、分产品、分地区", "营业收入比营业成本毛利率上年同期增减", "减少了个百分点", "主营业务收入万元主营业务成本万元毛利率", "提高个百分点", "毛利率营业收入比上营业成本年同期增减", "增个百分点","降个百分点","营业成本毛利率营业收入比上年同期增减", "分行业或产品","rrayBuffer(Autogen",  "rrayBuffer(Autogen,  按地区","rrayBuffer(Autogen", "期增减", "其他制造业","营业成本元毛利率", "rrayBuffer(单位：","rrayBuffer(、主营业务构成情况单位：元", "营业成本元毛利率营业收入比上年同期增减", "毛利率上年同增减", "主营业务分行业、产品情况表","主营业务分类别情况", "主营业务分行业和分产品情况的说明","业务类别", "年同期增减",  "上年同期增减",  "上年同期增减",  "上年同期增减", "主营业务产品情况", "营业收入比上营业成本比上年年同期增减同期增减", "营业成本比上年毛利率比上年同期增减同期增减", "福星惠誉·水岸国际", "上年同期增减",     "其他项目",  "营业收入营业成本毛利率比上年同期增减", "收入元",  "年月成本",  "毛利",  "——","年主营业务分行业分产品情况单位",  "境内小计", "境外小计","其他区域",  "未知",    "上升了个百分点", "比上年降低个百分点", "分行业、分产品",  "比上年增长个百分点", "并抵消", "营业成本元",   "增加了个百分点","、主营业务分地区情况", "营业收入营业成本毛利率比上年增,", "营业成本毛利率比上年增减", "内销", "外销",  "其他国际地区", "营业成本比上年","少了个百分点", "毛利率报告期", "分板块", "营业收入报告期", "营业收入上年同期经重述",  "业务",  "营业利润率毛利率", "营业利润率毛利率比上年增减", "境内小计", "境外小计",  "营业收入营业成本毛利率营业收入比上年增减", "降低个百分点",  "公司业务分行业情况", "币种","关联方交易价格的确定依据为市场价。", "联交易", "关联交易的定价原则", "主营业务收入比上年增减", "其它", "毛利率比上年增减百分点","毛利率比", "国际", "毛利率比上年同期增减个百分点", "上升", "下降",   "区域", "本期发生额", "毛利润率", "本期金额", "上年同期金额", "本期金额",
         "分行业或", "门店类型", "营直店", "商场专柜", "营直店", "专卖店", "省外地区","营业利润比上年同增减","年平均", "年~月","年度已审实现数", "年度预测数", "增减变动数", "、主营业务收入及成本","年预测数与年实际数增减比率", "年实际毛利率", "年预测毛利率", "年预测数与年实际数增减百分点","年已审数", "预测数", "年预测数与年已审数增减比率", "年预测数与年预测数增减比率", "年已审毛利率", "年预测数与年已审数增减百分点", "年预测数与年预测数增减百分点", "月已审数", "月预测数",  "收入占","成本元",
         "利率营业收入营业成本比上年增减","、主营业务分行业、分产品情况表", "分行业或分产品营业收入营业成本","千元", "其他收入",   "比上年增减", "营业利润率营业收入比上年增减","比上年同期增减",  "省内", "以上的主要产品", "省外","报告期内占公司营业收入或营业利润总额","分行业和产品", "比上年增减", "经营活动及其所属行业营业收入元营业成本元营业利润率","营业成本元营业利润率","营业利润率比上年增减百分点", "毛利率营业收入营业成本比上年增减", "比上年增减","下降了个百分点", "毛利率比", "上年增减", "不适用", "营业收入元", "营业成本毛利率营业收入比上年元同期增减",  "煤炭", "外贸区小计", "华南区小计", "华东区小计", "华中区小计", "西南区小计",
         "营业成本比","分行业分产品", "上年增减","增加百分点","年度", "毛利率比上", "年增减", "细分", "收入", "营业", "成本","年预测数与年实际数增减比率","主要变动原因说明","毛利率同比增减", "主营业务分产品情况", "营业收营业成本毛利率入比上", "营业成本比上年增减", "年年度","毛利元", "毛利占比","项目/年度","月累计", "产量", "万千升", "万吨","金额万元同比增长","年度前次评估本次评估"," 减少个百","营业利润率同比增减", "时间", "财务指标", "进入资产", "备考合并", "期","年营业收入", "年度营业收入", 
        "标准金", "营业成本毛利率营业收入比上年增减",  "不适用", "营业收入合计", "营业成本同比增减","变动额", "变动率", "小核酸产品","商业", "年份","毛利占比","年季度","占收入比", "内销收入", "外销收入", "营业外收入","营业外支出","产品或服务项目名", "以后年度","占营业收入比重","比上年同","占营业收入比例","期间费用计","分部营业收入/成本", "分部其他业务收入/成本","产品细分","年月未经审计","年以后年度", "出版", "发行", "内部抵销数","、出版","润率比上年同期增减","年毛利", "月毛利率", "年度毛利", 
         "其它产品",    "占比",  "营业收",   "全行业合计", "主营业务分产业板块情况", "营业成", "入比上年增减",  "入比上年增减", "本比上年增减", "增加个百", "分点", "营业收", "营业成", "入比上年增减", "本比上年增减", "增加个","不变", "抵消", "其他含出口",  "营业利", "润率", "营业利润率比","至稳定年", "销售量台", "上半年度","去年", "去年同期","年‐月","毛利率营", "营业成本比上年毛同期增减", 
        "营业成本营业利润率","减少个","其他业务收入", "、公司主营业务分行业、产品情况表", "人民币", "营业总收入", "营业总收入比", "本公司","品种", "影响毛利因素", "同比变化量", "影响毛利额","下降个百分比","指标", "金额万元", "营业收入占比", "利润总额", "净利润", "业业业业", "本营业收入","入住率/Occ", "平均每天房价/ADR", "元/间", "主营业务项目分类", "RMB",  "年平均","本年比上年增减","财务指标", "方法", "全额法A", "净额法B", "影响金额C=BA", "变化幅度D=BA/A", "变化幅度C=BA/A", "应收账款周","减少个百",
         "年月日/年度", "指标", "资金来源合计", "同比增长率","年月营业成本","年度营业成本", "年营业成本","本报告期", "上年同期", "其他地区","资产总额","销售码洋", "增长","上年同期增", "占收入比例",  "占比变化", "职工薪酬", "差旅费", "项目租车费", "办公费", "发电油费", "物料消耗", "硬件成本", "劳务外协费", "毛利率增幅","本期发生额营业收入", "上期发生额营业收入",
         "‐‐","类别", "预测年度", "销售费用", "营业收入占比", "管理费用", "财务费用",  "业收入",  "其他地区","数额", "增减变动","其他来源","潜能恒信", "海油工程", "中海油服", "恒泰艾普", "海默科技", "惠博普", "报告参数", "平均毛利率","贤妻", "谁是真英雄", "绝地枪王", "刺刀英雄", "如果爱可以重来", "金额元", "比重", "其他业务成本", "营业成本合计","比上年增减比上年增减","毛利贡献度","毛利贡献率","本年","年及永续","年占比", "财务指标", "方法","全额法A", "净额法B", "影响金额C=BA", "变化幅度D=BA/A", "变化幅度C=BA/A", 
         "细分", "营业", "收入", "营业", "成本构成项目", "本期占总成本比例", "上年同期占总成本比例", "本期金额较上年同期变动比例", "情况", "说明","毛利元", "毛利占比","年度月", "本期数", "上年同期数", "期数", "上年", "同期","年E", "年月的占比", "编制单位", "金额单位", "项目名称年实际年预测数数月实际数月预测数", "增减变动率", "年预测数月预测数", "年预测数月实际月数预测数",
         "成本", "同行业同领域产品毛利率情况", "减少个",  "出口业务", "比上年增减", "上年增减",   "年月月", "HUB","公司名称", "净利润", "总资产", "净资产", "山西振东泰盛制药有限公司", "山西振东开元制药有限公司", "金额元", "比重", "其他业务成本", "营业成本合计","差异额交易股改", "差异率", "股改评估营业成本率", "交易评估营业成本率", "成本率差异交易股改", "净利润", "交易评估预测数", "股改评估预测数","营收增长率","年~月净利润", "年净利润", "集团", "兴业", "君正", "均值",
         "山西振东道地药材开发有限公司", "山西振东医药有限公司", "北京振东光明药物研究院有限公司", "山西振东家庭健康护理用品有限公司", "山西振东道地连翘开发有限公司", "山西振东道地党参开发有限公司","年月日/年", "金额万元", "存货余额","本期发生额营业成本", "上期发生额营业成本","销售量万册", "去年", "京东", "唯品会", "天猫","年上半年", "重组前", "重组后模拟", 
         "年备考",     "本年发生数", "上年发生数","拟注入资产", "和平茂业", "净利润", "华强北茂业", "深南茂业", "东方时代茂业", "珠海茂业","截至十月十日止年度", "截至九月十日止九個月", "零年", "零五年", "零年", "上年同期数营业收入营业成本",
        "百分比除外","单位：人民币百万元","单位：人民币百万元","单位：百万元人民币（百分比除外）", "分行业与分产品","其他不可分行业","江苏通润","单位：万元","单位：人民币元","单位：人民币万元", "其他类","单位：元", "外地", "增长个百分点","出口销售", "国内销售","数据来源", "营业收入比上年","境外", "境内", "Q同比","同期增减", "单位万元" , "毛利率比上年同期增减幅度","其他行业", "营业利润比上年同期增减","上年同期无数据", "上年同期无数据", "其他不可分行业", "业务名称","营业成本千元","毛利率比上年同增减", "入入入入","小计", "毛利率比上年同期增减分行业", "毛利率营业按收营业成本", "入比上年比上年增增减","增加","分行业人民币", "毛利率比上年同期", "占总收入比", "毛利率营业收入比上年增减", "主营收入中占比", "金额", "数值", "其中", "华中", "营业收入比上年增", "名称", "上市公司调研报告·平潭发展", "年公司各项主营业务发展情况", "营业成本比上年增", "东北境外","营业收入百万元", "营业成本百万元", "营业收入同比", "毛利率同比变动", "公司上半年加大成本控制及产品结构上移遏制盈利下滑","分部，营业收入","个百分", "分部", "营业收入", "营业收入营业成本", "降低了个百分点", "提高了个百分点", "上升个百分点",  "营业收入万元", "营业成本万元", "毛利率营业收入比上年增减营业成本比上年增减","+ppt","ppt","个百分点","亿元","毛利率变化","主营业务分行业情况","百分点", "毛利率比上年同期增减百分点", "其他主营业务收入",  "营业收入比", "营业成本比上", "主营业务收入", "主营业务成本", "主营业务收入比上年同期增减", "主营业务成本比上年同期增减", "营业收入亿元", "营业成本亿元", "营业利润率比上年同期增减", "百分点", "比上年增", "毛利率比上年", "增减", "营业利润率", "营业利润率比上年增减", "其他产品","较少个百分点","下降个百分点","万元","百万元","年销量","销量同比营业收入","营业成本毛利率营业收入比上年同期增减营业成本比上年同期增减","其他","单位","营收占比","营收同比","营业成本同比","减少")
     
         val arr_del_3 = Array("金额占比", "截至年",  "日止个月",  "截至年月",  "百分比除外",  "百分比", "金额亿元","年月","本集团", "单位","所得税资产","占总额","–月占比", "年占比","年度","年月日","年业收入","业务线","年月金额占比","业务","集团","重述","在岗员工数量人", "分部税前利润", "年金额占比","利润总额","单位","截至年月日止个月期间", "年截至月日止半年度", "其他收入" ,"其中" ,"利息支出","总资产","总计息负债", "非计息负债", "总负债", "净利息收益率", "其他业务收入", "其他",  "营业收入", "小计","利息净收入",  "非利息净收入/支出",  "营业收入", "营业支出",  "资产减值损失", "营业外净收入/支出","分部利润", "所得税费用", "变动幅度", "营业收入合计", "金额", "其他业务","资产合计", "税前利润","营业收入","负债合计","项目", "其他","合计", "占比" ,"与上年增减" ,"较上年同期增减", "占业务总收入比重", "占业务总收入比重" ,"所占比例" ,"投资" ,"贷款","其他业务" ,  "主营业务收入人民币百万元","融资租赁","项目","年金额", "业务种类", "与上年增减","不适用","与去年同期相比增减","所占比例", "与上年增减", "、营业支出", "、营业利润", "、资产总额", "五、负债总额", "月占比","人民币百万元", "占总额百分比", "资产总额", "不含递延所得税资产","年金额", "营业收入", "资产总额不含递延所得税资产","税前利润", "合计", "总计","金额", "占比","金额")
         val j_xianjin =      bson_rdd_buffer.map{
      x => 
        {
          var xianjin_map1 :Map[String,String] = Map()
           var m2=Map[String,String]()
          x.foreach{
          case(key,value)=>{
             var k =0
             var companymapfeatures:Map[String,String] = Map()
              var xianjin_map :Map[String,String] = Map()
              var m1=Map[String,String]()
              val bson_buffer = ArrayBuffer[String]()
              val bson_buffer_yinhang = ArrayBuffer[String]()
               val xianjin_length = (value.length).toDouble
              val words_length = xianjin_rdd_arr.length.toDouble
                   for (j <- 0 until value.length) 
                  {  
                     var aa = 0 
                  for (i <- 0 until xianjin_rdd_arr.length)
                  { 
                    //普通情况
                    if (value(j).trim().equals(xianjin_rdd_arr(i).trim()) && value(j).trim().length()>0)
                      {
                        k = k+1
                       // bson_buffer_yinhang += value(j) //银行的时候需要，计算普通情况下一定不要加
                        aa = aa +1   //银行不需要，普通需要 
                      }
                    
                               }  
                    
                     //修改这个数组，普通和银行不一样
                     for (i <- 0 until arr_del.length)
                     {
                       if (value(j).trim().equals(arr_del(i).trim()))
                      {
                       
                        aa = aa + 1 
                      }
                     }
                     if ( aa == 0)
                     {
                       bson_buffer += value(j)
                     }
                              }
            val   bson_buffer_yinhang_dis = bson_buffer_yinhang.distinct
            val  bson_buffer1 =    bson_buffer.distinct
             val kmore = k.toDouble / xianjin_length
             val kmore1 = k.toDouble / words_length
             //银行情况下的判断
                //if ( k>10 && kmore > 0.55 && bson_buffer_yinhang_dis.length >2  && tag==4  && xianjin_length >3  && bson_buffer1.length <25  ){
             //普通分类情况下的判断
                if ( kmore > 0.75 && k > 19 && k <39 && tag==4  && xianjin_length >7.0 && bson_buffer1.length >1 ){
                    		 val bson_buffer_z = bson_buffer1 +="主营构成表"
                    		 val bson_buffer_z_s =   bson_buffer_z.toString().replace("一", "").replace("二", "").replace("三", "").replace("四", "").replace("—", "").replace("六", "")
                       //val f1 =   bson_buffer_z_s.lastIndexOf("0")  //普通情况下的取值
                       val f1 =   bson_buffer_z_s.lastIndexOf("0")   //求银行情况下的取值
                       val bson_buffer_z_s2 = bson_buffer_z_s.substring(f1+2, bson_buffer_z_s.length())
                       //val bson_buffer_z_s3 = bson_buffer_z_s2.substring(11, bson_buffer_z_s2.length())
                      // val bson_buffer_z_s3 = bson_buffer_z_s2.substring(11, bson_buffer_z_s2.length()).replaceAll("单位：元", "").replaceAll("单位：万元", "").replaceAll("单位：人民币万元", "").replaceAll("单位：人民币元", "")
                      m1 += (key -> bson_buffer_z_s2)
                    // xianjin_map += (key ->"主营构成表")
                 
                           }
            
                    //银行情况
                   /*  if (value(j).trim().equals(xianjin_rdd_arr(i).trim()) && value(j).trim().length()>0)
                      {
                        k = k+1
                        bson_buffer_yinhang += value(j) //银行的时候需要，计算普通情况下一定不要加
                        //aa = aa +1   //银行不需要，普通需要 
                      }
                    
                               }  
                    
                     //修改这个数组，普通和银行不一样
                     for (i <- 0 until arr_del_3.length)
                     {
                       if (value(j).trim().equals(arr_del_3(i).trim()))
                      {
                       
                        aa = aa + 1 
                      }
                     }
                     if ( aa == 0)
                     {
                       bson_buffer += value(j)
                     }
                              }
            val   bson_buffer_yinhang_dis = bson_buffer_yinhang.distinct
            val  bson_buffer1 =    bson_buffer.distinct
             val kmore = k.toDouble / xianjin_length
             val kmore1 = k.toDouble / words_length
             //银行情况下的判断
                if ( k>10 && kmore > 0.55 && bson_buffer_yinhang_dis.length >2  && tag==4  && xianjin_length >3  && bson_buffer1.length <25  ){
             //普通分类情况下的判断
               // if ( kmore > 0.75 && k > 19 && k <39 && tag==4  && xianjin_length >7.0 && bson_buffer1.length >1 ){
                    		 val bson_buffer_z = bson_buffer1 +="主营构成表"
                    		 val bson_buffer_z_s =   bson_buffer_z.toString().replace("一", "").replace("二", "").replace("三", "").replace("四", "").replace("—", "").replace("六", "")
                       //val f1 =   bson_buffer_z_s.lastIndexOf("0")  //普通情况下的取值
                       val f1 =   bson_buffer_z_s.lastIndexOf("0")   //求银行情况下的取值
                       val bson_buffer_z_s2 = bson_buffer_z_s.substring(f1+2, bson_buffer_z_s.length())
                      val bson_buffer_z_s3 = bson_buffer_z_s2.substring(11, bson_buffer_z_s2.length()).replaceAll("raw_data", "").replaceAll("单位：元", "").replaceAll("单位：万元", "").replaceAll("单位：人民币万元", "").replaceAll("单位：人民币元", "").replaceAll("单位：人民币百万元", "").replaceAll("（单位：人民币百万元）", "")
                      m1 += (key -> bson_buffer_z_s2)
                    // xianjin_map += (key ->"主营构成表")
                 
                           }*/
                    
            //证券情况
          /*if (value(j).trim().equals(xianjin_rdd_arr(i).trim()) && value(j).trim().length()>0)
                      {
                        k = k+1
                        bson_buffer_yinhang += value(j)
                        
                      }
                    
                               }  
                   
                              }
            val   bson_buffer_yinhang_dis = bson_buffer_yinhang.distinct
            val  bson_buffer1 =    bson_buffer.distinct
             val kmore = k.toDouble / xianjin_length
             val kmore1 = k.toDouble / words_length
            //证券情况判断
              if (  bson_buffer_yinhang_dis.length > 2 && bson_buffer_yinhang_dis.length < 8 && tag==4 ){
                     val bson_buffer_z = bson_buffer_yinhang_dis +="主营构成表"  //修改了成了匹配上txt文件里的字符串
                       val bson_buffer_z_s =   bson_buffer_z.toString().replace("一", "").replace("二", "").replace("三", "").replace("四", "").replace("—", "").replace("六", "")
                       //val f1 =   bson_buffer_z_s.lastIndexOf("0")  //普通情况下的取值
                       val f1 =   bson_buffer_z_s.lastIndexOf("0")   //求银行情况下的取值
                       val bson_buffer_z_s2 = bson_buffer_z_s.substring(f1+2, bson_buffer_z_s.length())
                       val bson_buffer_z_s3 = bson_buffer_z_s2.substring(11, bson_buffer_z_s2.length())
                      m1 += (key -> bson_buffer_z_s2)
                    // xianjin_map += (key ->"主营构成表")
                 
                           }
                         */  
                    else
                 {
                     xianjin_map += (key ->"")
                 }
                 xianjin_map.foreach
                   {
                     case (key ,value)=>
                   {
                       xianjin_map1 += (key -> value)
                    }
                       }
                 m1.foreach
                   {
                     case (key ,value)=>
                   {
                       m2 += (key -> value)
                    }
                       }
                   } 
                }
              //xianjin_map1
          m2
        }
    }
     j_xianjin
  }
  
}