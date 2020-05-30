import scala.reflect.runtime.universe
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.mean


// Load Data And Creating A Spark Dataframe
val bank_data = sc.textFile("C:/spark/banking_market_analysis/Project 1.csv")
val bank = bank_data.map(x => x.split(";"))
val bank_r = bank.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
case class Bank(age:Int, job:String, marital:String, education:String, defaultn:String, balance:Int,
housing:String, loan:String, contact:String, day:Int, month: String, duration:Int, campaign:Int, pdays:Int,previous:Int, poutcome:String, y:String)

val bank_clean = bank_r.map(
x => Bank(x(0).replaceAll("\"","").toInt,
x(1).replaceAll("\"","")
,x(2).replaceAll("\"","")
,x(3).replaceAll("\"","")
,x(4).replaceAll("\"","")
,x(5).replaceAll("\"","").toInt
,x(6).replaceAll("\"","")
,x(7).replaceAll("\"","")
,x(8).replaceAll("\"","")
,x(9).replaceAll("\"","").toInt
,x(10).replaceAll("\"","")
,x(11).replaceAll("\"","").toInt
,x(12).replaceAll("\"","").toInt
,x(13).replaceAll("\"","").toInt
,x(14).replaceAll("\"","").toInt
,x(15).replaceAll("\"","")
,x(16).replaceAll("\"","")
)
)

val bank_df = bank_clean.toDF()
bank_df.createOrReplaceTempView("bank")

val builder=new org.apache.spark.sql.SparkSession.Builder()
val sparkSession=builder.getOrCreate()
val sqlContext=sparkSession.sqlContext;


// Marketing Success And Failure Rate
val success = sqlContext.sql("select round((x.subscribed/y.total)*100,3) as success_rate from (se
lect count(*) as subscribed from bank where y='yes') x,(select count(*) as total from bank) y").show() 
val failure = sqlContext.sql("select round((x.not_subscribed/y.total)*100,3) as failure_rate from
(select count(*) as not_subscribed from bank where y='no') x,(select count(*) as total from bank) y").show()


// Maximum, Mean, And Minimum Age Of The Average Targeted Customer
sqlContext.sql("select max(age) as max_age,min(age) as min_age,round(avg(age),2) as avg_age from bank").show() 

//Average Balance, Median Balance Of Customers
val average_balance = sqlContext.sql("select round(avg(balance),2) as average_balance from bank").show()  
val median_balance = sqlContext.sql(“SELECT percentile_approx(balance,0.5) as median_balance FROM bank”).show()

// Role Of Age In Marketing Subscription For Deposit
val age = sqlContext.sql("select age,count(*) as number from bank where y='yes' group by age order by number desc ").show()
val response_by_age = sqlContext.sql("select x.age,round((x.subscribed/y.total)*100,2) as 
percent from (select age,count(age) as subscribed from bank where y='yes' group by age) x,
(select age,count(age) as total from bank group by age) y where x.age = y.age order by
 percent desc").show()

// Role Of Marital Status For A Marketing Subscription To Deposit
val marital = sqlContext.sql("select marital,count(*) as number from bank where y='yes' 
groupby marital order by number desc ").show()
val response_by_marital = sqlContext.sql("select x.marital,(x.subscribed/y.total)*100 as 
percent from (select marital,count(marital) as subscribed from bank where y='yes' group 
by marital) x,(select marital,count(marital) as total from bank group by marital) y 
where x.marital = y.marital order by percent desc").show()

// Does Age And Marital Status Together Matter For A Subscription In The Deposit Scheme ?
val age_marital = sqlContext.sql("select age,marital,count(*) as number from bank where 
y='yes' group by age,marital order by number desc ").show()
val response_by_marital_age =  sqlContext.sql("selectx.marital,x.age,
round((x.subscribed/y.total)*100,2) as percent from (select marital,age,count(marital) 
as subscribed from bank where y='yes' group by marital,age) x,(select marital,age,count(marital)
as total from bank group by marital,age) y where x.marital = y.marital and x.age = y.age order by percent desc").show()


// Let Us Find The Effect Of The Marketing Campaign On Different Age Groups

// Grouping By Age
 val age_RDD = sqlContext.udf.register("age_RDD",(age:Int) => {
 if (age >= 18 && age <= 30)
"Young_Adult"
else if (age > 30 && age <= 45)
"Adult"
else if (age>45 && age <=60)
"Middle_aged"
else
“Old”
})


// Pipeline Of Stringindexer Based On Age Group
val bank_DF1 = bank_df.withColumn("age",age_RDD(bank_df("age")))
bank_DF1.createOrReplaceTempView("bank_DF1")

val age_index = new org.apache.spark.ml.feature.StringIndexer().setInputCol("age").setOutputCol("ageIndex")

// Fit And Transform
var model_fit = age_index.fit(bank_DF1)
model_fit.transform(bank_DF1).select("age","ageIndex").show()	
