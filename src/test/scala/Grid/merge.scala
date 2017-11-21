package Grid

import scala.collection.mutable.ArrayBuffer


import com.mongodb.casbah.Imports._  
object merge {
  def main(args:Array[String])
  {
    //mongo数据库连接
    //String sURI = String.format("mongodb://%s:%s@%s:%d/%s", "bigdata", "defu33%40rEpam", "121.40.131.65", 3717, "cr_data");
    val url = format("mongodb://%s:%s@%s:%d/%s", "bigdata", "bre5Uc#yu_hu", "10.11.255.122", 27017, "cr_data"); 
    val uri = MongoClientURI(url)  
    val mongoClient =  MongoClient(uri)  
    val db=mongoClient("cr_data")  
    val coll=db("merge_grid")  
  //普通情况——合并3表情况
    val st1 = "分行业,营业收入,营业成本,毛利率(%),营业收入比上年增减(%),营业成本比上年增减(%),毛利率比上年增减(%),制造业,86288088.25,61546825.63,28.67,27.95,20.27,增加4.55个百分点"
    val st2 = "分产品,营业收入,营业成本,毛利率(%),营业收入比上年增减(%),营业成本比上年增减(%),毛利率比上年增减(%),镁合金类产品,79323506.17,54954136.64,30.72,26.80,17.66,增加5.38个百分点,铝合金类产品,6964582.08,6592688.99,5.34,42.60,47.48,减少3.13个百分点"
    val st3 = "分地区,营业收入,营业成本,毛利率(%),营业收入比上年增减(%),营业成本比上年增减(%),毛利率比上年增减(%),华北地区,22761570.44,15680953.74,31.11,112.22,102.58,增加3.28个百分点,华东地区,55788774.61,40478744.93,27.44,6.68,0.07,增加4.79个百分点,华中地区,,,,-100.00,-100.00,,西南地区,6451155.06,4752192.81,26.34,55.02,64.25,减少4.14个百分点,西北地区,,,,-100.00,-100.00,,境外,1286588.14,634934.14,50.65,726.50,944.33,减少10.29个百分点"
   //普通情况--合并2表情况
    val st2_1 ="分行业,营业收入,营业成本,毛利率(%),营业收入比上年增减(%),营业成本比上年增减(%),毛利率比上年增减(%),制造业,291167321.16,206090297.83,29.22%,-37.36%,-43.52%,增加7.72个百分点,房地产业及物业服务,2648685085.61,2142563441.80,19.11%,38.48%,59.66%,减少10.73个百分点,建筑业,2532340.61,1975772.93,21.98%,-43.34%,-50.17%,增加10.70个百分点,主营业务分产品情况,主营业务分产品情况,主营业务分产品情况,主营业务分产品情况,主营业务分产品情况,主营业务分产品情况,主营业务分产品情况,,分产品,营业收入,营业成本,毛利率(%),营业收入比上年增减(%),营业成本比上年增减(%),毛利率比上年增减(%),专用车,291167321.16,206090297.83,29.22%,-37.36%,-43.52%,增加7.72个百分点,房地产销售及物业服务,2648685085.61,2142563441.80,19.11%,38.48%,59.66%,减少10.73个百分点,建筑装饰工程,2532340.61,1975772.93,21.98%,-43.34%,-50.17%,增加10.70个百分点"
    val st2_2 = "地区,营业收入,营业收入比上年增减(%),重庆,2077678064.04,38.48%,武汉,673045780.48,2.00%,其他,191660902.86,-13.61%"
    //证券情况--海通证券2表情况
    val st1_z = "分行业,营业收入,营业支出,毛利率(%),营业收入比上年增减(%),营业支出比上年增减(%),毛利率比上年增减(%),证券及期货经纪业务,3254694326.92,1269480927.82,61.00,-25.31,-18.73,减少3.16个百分点,投资银行业务,1051866623.85,332762441.34,68.36,19.07,12.95,增加1.71个百分点,资产管理业务,1017608755.41,426503880.49,58.09,73.41,11.98,增加23.00个百分点,自营业务,2697841874.94,1909955577.51,29.20,-5.09,-4.62,减少0.36个百分点,直投业务,1295137002.33,98178713.84,92.42,146.28,237.64,减少2.05个百分点,管理部门及其他,1640517360.14,1202153914.61,26.72,389.41,25.56,增加212.34个百分点,海外业务,1885588696.55,1739383822.43,7.75,9.50,34.90,减少17.37个百分点,融资租赁业务,1225485520.16,465083585.58,62.05,28.94,7.25,增加7.67个百分点"
    val st2_z ="分地区,营业收入,营业支出,毛利率(%),营业收入比上年增减(%),营业支出比上年增减(%),毛利率比上年增减(%),上海,299562549.65,149891504.45,49.96,-37.35,-14.14,减少13.52个百分点,浙江,197611834.80,61690614.44,68.78,-39.98,-37.91,减少1.04个百分点,黑龙江,194147145.87,92310276.76,52.45,-37.18,-22.87,减少8.82个百分点,江苏,138542938.43,67907458.13,50.98,-42.07,-20.87,减少13.13个百分点,山东,108433006.71,47276234.73,56.40,-35.20,-21.91,减少7.42个百分点,其他地区分支机构,763615969.19,402306919.36,47.32,-40.85,-19.65,减少13.90个百分点,公司总部及境内子公司,10481238019.10,4882736033.32,53.41,36.68,5.70,增加13.65个百分点,境内小计,12183151463.75,5704119041.19,53.18,16.23,0.78,增加7.18个百分点,海外业务,1885588696.55,1739383822.43,7.75,9.50,34.90,减少17.37个百分点,抵销,-1254784094.74,-3538407.21,99.72,-,-,减少0.15个百分点,合计,12813956065.56,7439964456.41,41.94,6.08,7.06,减少0.53个百分点"
   //银行情况--3表 : jc_1203196857
    val st1_y = "项目,2016年,批发金融业务,40040,零售金融业务,45099,其他业务,6176,合计,78963"
    val st2_y = "贷款,151236,存放中央银行,8170,拆借、存放等同业业务,10354,投资,45721,手续费及佣金收入,66003,其他业务,13565,合计,295049"
    val st3_y = "总行,43532,长江三角洲地区,10312,环渤海地区,5965,珠江三角洲及海西地区,11856,东北地区,1436,中部地区,634,西部地区,3559,境外,1500,附属机构,7287,合计,78963"
    //保险
    val st1_b ="已赚保费,336270,寿险业务,300516,健康险业务,28715,意外险业务,7039,投资收益*,58271,公允价值变动损益,3713,汇兑损益,42,其他业务收入,3212"
    val st2_b = "江苏,34945,广东,28928,山东,24893,浙江,22328,河北,20353,中国境内其他分公司,214520,合计,345967"
    //普通--合并3表
    //val arr11 = merge_three_grid(st1,st2,st3)//2016年年度报告 (13)-丰华.pdf，例子
    //证券--合并2表——海通
    //val arr11_z = merge_two_zhenquan(st1_z,st2_z)
    //银行--2表合并
    //val arr11_y = merge_3_bank(st1_y,st2_y,st3_y)
    //保险--2表合并
     //val arr_b_1 = merge_2_baoxian(st1_b,st2_b)
    //普通--合并2表
    val arr22 = merge_two_grid(st2_1,st2_2)
   /*for ( i <- 0 until arr22.length) 
   {
     print(arr22(i)+",")
   }
   val a=MongoDBObject("merge_arr"->arr11,"src_id"->"jc_1202260970") 
   val b=MongoDBObject("merge_arr"->arr22,"src_id"->"jc_1203870527") 
   val c=MongoDBObject("merge_arr"->arr11_z,"src_id"->"jc_1203896344") 
    val c=MongoDBObject("merge_arr"->arr_b_1,"src_id"->"jc_1203864536") 
    coll.insert(c)
    
    */
   
  }
  
  def merge_two_grid(st2_1:String,st2_2:String):ArrayBuffer[String]=
  {
    val exam = "营业收入,营业成本,毛利率(%),营业收入比上年增减(%),营业成本比上年增减(%),毛利率比上年增减(%)"
    val exam_arr = exam.split(",")
   
    val arr1_split = st2_1.split("主营业务分产品情况")
    val arr1_1_1 = ArrayBuffer[String]()
    val arr2_2_2 = ArrayBuffer[String]()
     if (st2_1.contains("分产品"))
     {
        val st2_1_1 = st2_1.indexOf("分产品")
        val st2_1_1_1 = st2_1.subSequence(0, st2_1_1).toString()
        val st2_1_1_2 = st2_1.subSequence(st2_1_1, st2_1.length()).toString()
        val st2_1_1_1_1 = st2_1_1_1.split(",")
         val st2_1_1_2_2 = st2_1_1_2.split(",")
        for (i <- 0 until st2_1_1_1_1.length)
        {
          arr1_1_1 += st2_1_1_1_1(i)
        }
        for (i <- 0 until st2_1_1_2_2.length)
        {
          arr2_2_2 += st2_1_1_2_2(i)
          
        }
        println("st2_1_1_1st2_1_1_1"+st2_1_1_1)
        println("st2_1_1_1st2_1_1_1"+st2_1_1_2)
     }
   
    
    val arr1 = st2_1.split(",")
    val arr2 = st2_2.split(",")
    val col_1 = ArrayBuffer[String]() //第一行数据
    val col_1_1 = ArrayBuffer[String]()//第二行数据
  val exam_next = ArrayBuffer[Int]()
    for (i <- 0 until 3)
    {
      col_1 += arr2(i)
    }
    for (i <- 3 until arr2.length)
    {
      col_1_1 += arr2(i)
    }
    val exam_next_ = ArrayBuffer[String]()
    for (i <- 0 until exam_arr.length)
    {
      val col_2 = ArrayBuffer[String]()
      for ( j <- 0 until col_1.length)
      {
        if (exam_arr(i).equals(col_1(j)))
        {
          col_2 += col_1(j)
          exam_next_ += col_1(j)
        }
      }
      if (col_2.isEmpty)
        {
          exam_next += i
          exam_next_ += exam_arr(i)
        }
    }
   exam_next_.clear()
    for (i <- 0 until col_1_1.length)
    {
        if (i ==1 || i==2 || i==4 || i==5 || i==7 || i==8)
        {
          exam_next_ += col_1_1(i)
          exam_next_ += ""
          exam_next_ += ""
        }else
          exam_next_ += col_1_1(i)
      
    }
  // for (i <- 0 until exam_next_.length)print(exam_next_(i)+",")
    val arr1_1 = arr1_split(0).split(",")//行业
    val arr1_2 = arr1_split(7).split(",")//产品
    val arr1_2_2 = ArrayBuffer[String]()
    val arr1_2_3 = ArrayBuffer[String]()
     //去重产品数组前面空的情况
    for (i <- 0 until arr1_2.length)
    {
      if ( i == 0 || i ==1 && arr1_2(i).equals(""))
          {
         arr1_2_3 += arr1_2(i)
          }
      else
      {
        arr1_2_2 += arr1_2(i)
      }
    }
     val arr11 = ArrayBuffer[String]()
   if (st2_1.contains("行业"))
   {
       arr11 += "项目类型"
       arr11 += "项目名称"
   }
   for (i <- 0 until arr1_1.length)
   {
    {
        if ( i !=0  )
            {
              arr11 += arr1_1(i)
              if (i == 6 || i == 13 || i == 20 || i == 27 || i == 34)
              {
                arr11 += "分行业"
              }
            }
    }
  }
   arr11.remove(arr11.length-1)
   for (i <- 7 until arr1_2_2.length)
   {
              if (i == 7 || i == 14 || i == 21 || i == 28 || i == 35)
              {
                arr11 += "分产品"
                arr11 += arr1_2_2(i)
              }
              else
              {
                arr11 += arr1_2_2(i)
              }
  }
   for ( i <- 0 until exam_next_.length)
   {
     if (i == 0 || i == 7 || i == 14 || i == 21 || i == 28 || i == 35)
              {
                arr11 += "分地区"
                arr11 += exam_next_(i)
              }
              else
              {
                arr11 += exam_next_(i)
              }
   }
   for (i <- 0 until arr11.length)
   {
     print(arr11(i)+",")
   }
   arr11
  }
  def merge_three_grid(st1:String,st2:String,st3:String):ArrayBuffer[String]=
  {
    val arr1 = st1.split(",")
    val arr2 = st2.split(",")
    val arr3 = st3.split(",")
   val classfy_arr = Array("行业")
   val arr11 = ArrayBuffer[String]()
   if (st1.contains("行业"))
   {
       arr11 += "项目类型"
       arr11 += "项目名称"
   }
 val com_ = hangye(arr11,arr1)
 val com_product_ = product(com_,arr2)
 val arr_all = diqu(com_product_,arr3)
 for (i <- 0 until arr_all.length)
 {
   print(arr_all(i)+"-")
 }
   arr11 
  }
  
  def hangye(arr11:ArrayBuffer[String],arr1:Array[String]):ArrayBuffer[String]={
     for (i <- 0 until arr1.length)
   {
    {
        if ( i !=0  )
            {
              arr11 += arr1(i)
              if (i == 6 || i == 13 || i == 20 || i == 27 || i == 34 || i == 41 || i == 48 || i == 55 || i == 62 || i == 69)
              {
                arr11 += "分行业"
              }
            }
    }
  }
     arr11.remove(arr11.length-1)
    arr11 
  }
  //3表合并地区情况
  def diqu(arr11:ArrayBuffer[String],arr3:Array[String]):ArrayBuffer[String]={
     for (i <- 7 until arr3.length)
   {
    {
        if (i == 7 || i == 14 || i == 21 || i == 28 || i == 35 || i == 42 || i == 49 || i == 56 || i == 63 || i == 70 || i == 77 || i == 84 || i == 91 || i == 98 || i == 105)
              {
                arr11 += "分地区"
                arr11 += arr3(i)
              }
              else
              {
                arr11 += arr3(i)
              }
        
    }
  }
     arr11
  }
  //3表合并产品情况
  def product(arr11:ArrayBuffer[String],arr2:Array[String]):ArrayBuffer[String]=
  {
     for (i <- 7 until arr2.length)
   {
    {
              if (i == 7 || i == 14 || i == 21 || i == 28 || i == 35 || i == 42 || i == 49 || i == 56)
              {
                arr11 += "分产品"
                arr11 += arr2(i)
              }
              else
              {
                arr11 += arr2(i)
              }
    }
  }
     arr11
  }
  
  
  
  
  
  
  
  
  
  /*def merge_2_baoxian(st1_b:String,st2_b:String):ArrayBuffer[String]=
  {
    val arr_1 = st1_b.split(",")
    val arr_2 = st2_b.split(",")
    val arr11 = ArrayBuffer[String]()
    
        
         arr11 += "项目类型"
         arr11 += "项目名称"
         arr11 += "营业收入"
         arr11 += "营业成本"
         arr11 += "毛利率(%)"
         arr11 += "营业收入比上年增减(%)"
         arr11 += "营业成本比上年增减(%)"
          arr11 += "毛利率比上年增减(%)"
           arr11 += "分行业"
         for (i <- 0 until arr_1.length)
 {
     
        if (i==1 || i ==3 || i==5 || i==7 || i==9 || i==11 || i==13 || i==15 || i==18 || i==21 )
        {
          arr11 += arr_1(i)
          arr11 += ""
          arr11 += ""
          arr11 += ""
          arr11 += ""
          arr11 += ""
          //arr11 += ""
          arr11 += "分行业"
          
        }else
        {
          arr11 += arr_1(i)
        }
 }
        arr11.remove(arr11.length-1)
  for (i <- 0 until arr_2.length)
    {
    if (i==0 )
     {
       arr11 += "分地区"
     }
        if (i==1 || i ==3 || i==5 || i==7 || i==9 || i==11 || i==13 || i==15 || i==17 || i==19 || i==21 )
        {
          arr11 += arr_2(i)
          arr11 += ""
          arr11 += ""
          arr11 += ""
          arr11 += ""
          arr11 += ""
          //arr11 += ""
          arr11 += "分地区"
          
        }else
        {
          arr11 += arr_2(i)
        }
    }
    arr11.remove(arr11.length-1)
  val arr_ti = ArrayBuffer[String]()
   for(i <- 0 until arr11.length)
   {
    
     if (i==3)
     {
       arr_ti +="营业成本".trim()
     }
     if (i==4)
     {
       arr_ti +="毛利率(%)".trim()
     }
     if (i==5)
     {
       arr_ti +="营业收入比上年增减(%)".trim()
     }
     if (i==6)
     {
       arr_ti +="营业成本比上年增减(%)	".trim()
     }
     if (i==7)
     {
       arr_ti +="毛利率比上年增减(%)".trim()
     }
     arr_ti += arr11(i)
   }
  arr_ti.remove(4)
  arr_ti.remove(5)
  arr_ti.remove(6)
  arr_ti.remove(7)
  arr_ti.remove(8)
  for (i <- 0 until arr_ti.length)print(arr_ti(i)+",")
     arr_ti
  }
  def merge_3_bank(st1_y:String,st2_y:String,st3_y:String):ArrayBuffer[String]=
  {
    val arr_1 = st1_y.replaceAll("2016年", "营业收入").replaceAll("项目", "项目名称").split(",")
    val arr_2 = st2_y.split(",")
    val arr_3 = st3_y.split(",")
     val arr11 = ArrayBuffer[String]()
     if (st1_y.contains("项目"))
     {
         arr11 += "项目类型"
        
     }
    val col_1 = ArrayBuffer[String]()
    for (i <- 1 until 2)
    {
      col_1 += arr_1(i)
    }
   
    for (i <- 0 until col_1.length)print(col_1(i)+"**")
    val exam = "营业收入,营业成本,毛利率(%),营业收入比上年增减(%),营业成本比上年增减(%),毛利率比上年增减(%)"
    val first_low = "项目类型,项目名称,营业收入,营业成本,毛利率(%),营业收入比上年增减(%),营业成本比上年增减(%),毛利率比上年增减(%)"
    val first_low_arr = first_low.split(",")
    val exam_arr = exam.split(",")
    val exam_next_ = ArrayBuffer[String]()
    val exam_next = ArrayBuffer[Int]()
    for (i <- 0 until exam_arr.length)
    {
      val col_2 = ArrayBuffer[String]()
      for ( j <- 0 until col_1.length)
      {
        if (exam_arr(i).equals(col_1(j)))
        {
          col_2 += col_1(j)
          exam_next_ += col_1(j)
        }
      }
      if (col_2.isEmpty)
        {
          exam_next += i
          exam_next_ += exam_arr(i)
        }
    }
    for (i <- 0 until exam_next.length)print(exam_next(i)+"--")
   exam_next_.clear()
   // val st1_y = "项目,2016年,批发金融业务,40040,零售金融业务,45099,其他业务,6176,合计,78963"
   for (i <- 0 until arr_1.length)
 {
     
        if (i==1 || i ==3 || i==5 || i==7 || i==9 )
        {
          arr11 += arr_1(i)
          arr11 += ""
          arr11 += ""
          arr11 += ""
          arr11 += ""
          arr11 += ""
          //arr11 += ""
          arr11 += "分行业"
          
        }else
        {
          arr11 += arr_1(i)
        }
    }
  arr11.remove(arr11.length-1)
  for (i <- 0 until arr_3.length)
    {
    if (i==0 )
     {
       arr11 += "分地区"
     }
        if (i==1 || i ==3 || i==5 || i==7 || i==9 || i==11 || i==13 || i==15 || i==17 || i==19 || i==21 )
        {
          arr11 += arr_3(i)
          arr11 += ""
          arr11 += ""
          arr11 += ""
          arr11 += ""
          arr11 += ""
          //arr11 += ""
          arr11 += "分地区"
          
        }else
        {
          arr11 += arr_3(i)
        }
    }
  arr11.remove(arr11.length-1)
   for (i <- 0 until arr_2.length)
    {
    if (i==0 )
     {
       arr11 += "分产品"
     }
        if (i==1 || i ==3 || i==5 || i==7 || i==9 || i==11 || i==13 || i==15 || i==17 || i==19 || i==21 )
        {
          arr11 += arr_2(i)
          arr11 += ""
          arr11 += ""
          arr11 += ""
          arr11 += ""
          arr11 += ""
          //arr11 += ""
          arr11 += "分产品"
          
        }else
        {
          arr11 += arr_2(i)
        }
    }
  arr11.remove(arr11.length-1)
  val arr_ti = ArrayBuffer[String]()
   for(i <- 0 until arr11.length)
   {
     if (i==3)
     {
       arr_ti +="营业成本".trim()
     }
     if (i==4)
     {
       arr_ti +="毛利率(%)".trim()
     }
     if (i==5)
     {
       arr_ti +="营业收入比上年增减(%)".trim()
     }
     if (i==6)
     {
       arr_ti +="营业成本比上年增减(%)	".trim()
     }
     if (i==7)
     {
       arr_ti +="毛利率比上年增减(%)".trim()
     }
     arr_ti += arr11(i)
   }
  arr_ti.remove(4)
  arr_ti.remove(5)
  arr_ti.remove(6)
  arr_ti.remove(7)
  arr_ti.remove(8)
  for (i <- 0 until arr_ti.length)print(arr_ti(i)+",")
     arr_ti
  }
  
  def merge_two_zhenquan(st1_z:String,st2_z:String):ArrayBuffer[String]=
    {
      val arr_1 = st1_z.split(",")
      val arr_2 = st2_z.split(",")
      val classfy_arr = Array("行业")
     val arr11 = ArrayBuffer[String]()
     if (st1_z.contains("行业"))
     {
         arr11 += "项目类型"
         arr11 += "项目名称"
     }
     
     val com_ = hangye(arr11,arr_1)
     // arr11.remove(arr11.length-1)
     val com_diqu = diqu(com_,arr_2)
    
     com_diqu
    }
      
  */
  
  
  
  
}