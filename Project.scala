import java.sql.{DriverManager, PreparedStatement}
import scala.io.Source
import java.nio.file.{FileSystems, Files, Path, Paths, StandardOpenOption, StandardWatchEventKinds, WatchService}
import java.io.File
import java.nio.file.WatchEvent.Kind
import java.nio.file.StandardWatchEventKinds._
import java.time.format.{DateTimeFormatter, DateTimeParseException}
import java.time.{Instant, LocalDate, LocalDateTime, ZoneOffset}
import java.time.temporal.ChronoUnit
import scala.jdk.CollectionConverters._
import scala.math.ceil
import scala.util.Using

object Project extends App {
//here made 6 check functions and 6 discount corresponding functions, so 6 rules, calculation happen only when first check is true
//Declaring the first check function which is wine and cheese
  private def check1(list: List[String]): Boolean = {                         // all functions takes list of string, and return boolean if it's check, return double if not
     val item = list(1).split(" ")                                     // taking the second column in list which has the cheese or wine and splitting the first word
    if (item(0).toUpperCase == "WINE" || item(0).toUpperCase == "CHEESE") {   // incase the first word upper (to handle if Wine or wine or WINE cases) equals cheese or wine
      true
    } else false
  }
//the first discount function for the corresponding check1 function which gives 5% for wine word, 10% for cheese
  private def dis1(list: List[String]): Double = {
    val item = list(1).split(" ")                                      // splitting the first word as uppove
    item(0).toUpperCase match {                                               // matching the word incase wine 5% and incase cheese 10%
      case "WINE" => 0.05
      case "CHEESE" => 0.10
      case _ => 0.0                                                           //else isn't useful but just for generlaization incase any error happened in the check, or something added/removed
    }
  }
  //check 2 declaration for time zone
  private def check2(list: List[String]): Boolean = {
    val timestamp = Instant.parse(list.head).atOffset(ZoneOffset.UTC).toLocalDate    //because time stamp is in UTC zone, we can parse it directly to be normal date formate as given
    //here had a problem, the file date expiry format, is different than the laptops general format so any modification in excel file, or if source of files is different, exception occure, so tried to handle both cases
    val formatter = DateTimeFormatter.ofPattern("M/d/yyyy")                  //first case is the laptop formatting , incase any edit happened in file, but the source format will through exception? handled in catch clause
    val expiry = try {
      LocalDate.parse(list(2), formatter)                                            //here parsing the date with first format
    } catch {
      case _: DateTimeParseException =>
        val formatter2 = DateTimeFormatter.ofPattern("yyyy-MM-dd")           //incase exception, changing the format
        LocalDate.parse(list(2), formatter2)                                         //and reading it here
    }
    ChronoUnit.DAYS.between(timestamp, expiry) <= 30                                 //now calculating the difference date and return true incase it's less than 30
  }

  private def dis2(list: List[String]): Double = {
    val timestamp = Instant.parse(list.head).atOffset(ZoneOffset.UTC).toLocalDate    //same reading file as above
    val formatter = DateTimeFormatter.ofPattern("M/d/yyyy")
    val expiry = try {
      LocalDate.parse(list(2), formatter)
    } catch {
      case _: DateTimeParseException =>
        val formatter2 = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        LocalDate.parse(list(2), formatter2)
    }
    val daysLeft = ChronoUnit.DAYS.between(timestamp, expiry)
    if (daysLeft > 0)                                                               // because some products are already expired so no discount for them
    (30 - daysLeft) * 0.01                                                          //discount is the remaining of 30 so if 3 days left, discount is 27%
    else
      0                                                                             // incase product already expired? so discount is 0%
  }
  // the third check which is for specific date (2023-03-23)
  def check3(list: List[String]): Boolean = {
    list.exists(_.startsWith("2023-03-23"))                //checking if the timestamp starts with that date , return true if yes
  }
// the third function for the last check
  def dis3(list: List[String]): Double = {
    0.5                                                    //this is called only when date if right, then return 0.5
  }
// 4th check for quantity when more than 5
  def check4(list: List[String]): Boolean = {
    if (list(3) == "quantity") {                           //first for the header, return false because it's a string, when converting to double will be error (didn't remove the header because tried first to parse it in excel and wanted it to stay functional)
      false
    } else {
      list(3).toDouble > 5.0                               //checking if amount more than 5, return true
    }
  }
//4th function for the last check
  def dis4(list: List[String]): Double = {
      list(3).toDouble match {                            //here matching, case more than 14 (15+) then discount 0.1 and if not, then if more than 9 takes 0.07 if not then 0.05, and of course if less than 5 won't enter the function
        case x if x > 14 => 0.1
        case x if x > 9 => 0.07
        case x if x > 5 => 0.05
    }
  }
// the fifth check for app
  private def check5(list: List[String]): Boolean = {
    list.lift(5).contains("App")                        //if the 5th column has word App, it will return true, and lift is function incase it's out of bound (if they removed that column in future) then it return option of none, so no exception occure and just always return false
  }

  private def dis5(list: List[String]): Double = {
    val quantity = list(3).toInt
    if (quantity > 0) {                                //checking first if the quantity is more than 0 becuase any errors in data where quantity is negative!
      val roundedQuantity = (ceil((quantity - 1) / 5) + 1).toInt * .05         //first we subtract the quantity by 1, so when we ceil it go to the lower value (although ceiling should ceil to upper value, but here in scala it ceil to lower value idk why) then divide by 5, add by 1 and convert to int to remove the decimal and this from 1 to 5, will be 1 , from 5 to 10 will be 2, then will multiply by 0.05 to get the dsicount
      roundedQuantity
    } else 0
  }
  //now last check
  private def check6(list: List[String]): Boolean = {
    list.lift(6).contains("Visa")                      //checking first if the payment is Visa return true
  }
  private def dis6(list: List[String]): Double = {
    0.05                                                //return 0.05 if the check if true
  }
  val calFinalPrice: (Double, Double) => Double = (price, discount) => {
    if (discount > 0) price * (1 - discount) else price //function for calculating the final price from the discount, incase if there is discount, if not then return normal price
  }
  //declaring function for the log message to append it into the log file
  private def logMsgFile(types : String, filename: String, rows: Int): Unit = {    //it takes the type which is reading or proccessing, the file name, and number of rows
    val logMessage = s"at ${LocalDateTime.now()}    $types $rows  rows from file  $filename \n"   //here the message it self
    Files.write(Paths.get(logFilePath), logMessage.getBytes(), StandardOpenOption.APPEND)         //here appending it into the log file
  }
  //declaring case class , to assign in it the 2 functions as elements (as an object like data), case class to be assigned easily
  case class DiscountRule(check: List[String] => Boolean, applyDiscount: List[String] => Double)
  //declaring list that contains the checks and discounts, so we can apply the check first then the discount, here we can add any number of discount rules anytime , just declaring it up and putting it here
  val discountRules = List(
    DiscountRule(check1, dis1),
    DiscountRule(check2, dis2),
    DiscountRule(check3, dis3),
    DiscountRule(check4, dis4),
    DiscountRule(check5, dis5),
    DiscountRule(check6, dis6)
  )
  private val logFilePath = "E:/scalaproject/log.txt"                 // declare the logfile path
  private val watchedFolderPath = "E:/scalaproject/watched_folder/"   // declaring the folder path where we put files in
  private val watchedFolder = new File(watchedFolderPath)             //and that's the folder it self declaring
  if (!watchedFolder.exists()) {
    watchedFolder.mkdir()                                             //in case folder wasn't made yet in the project folder, it automatically make it
  }
  private val logFile = new File(logFilePath)                         //in case no log file made, it automatically make a log file
  if (!logFile.exists()) {
    logFile.createNewFile()
  }


  private val watcher: WatchService = FileSystems.getDefault.newWatchService()    //declaring the listener that will watch for new files
  private val watchPath: Path = Paths.get(watchedFolderPath)                      //declaring the path of watcher
  watchPath.register(watcher, StandardWatchEventKinds.ENTRY_CREATE)
  while (true) {
    val key = watcher.take()                                                      //declaring the key , where indicate when watcher has files and will reset it at the end to take more files
    val events = key.pollEvents().asScala.toList                                  // events is the files in every round
    for (event <- events) {                                                       //looping over the events
      val kind = event.kind().asInstanceOf[Kind[Any]]                             //for each new file, the code reads its contents and converts them into a list of lists of strings
      if (kind == ENTRY_CREATE) {
        val filename = event.context().toString                                    //getting the file name and assign it in variable
        val file = new File(watchedFolderPath + filename)                          //the full file path
        if (file.isFile) {                                                         // checking that file is readable file
          var retry = true                                                         //making a variable as retry, in case it failed to read file, it retry reading it 3 times, if didn't work then will print that it failed
          var retryCount = 0                                                       //starting the retry count by 0
          while (retry && retryCount < 3) {                                        //while retry true, and counter less than 3, it will keep trying to read it
            try {
              Using(Source.fromFile(file)) { source =>                             //using block clears the memory and makes the JavaTM closes the file to be deleted
                val lines = source.getLines.toList                                 // retreiving the lines from file
                val listofLists = lines.map(_.split(",").toList)             //splitting it into list of lists
                println(s"File $filename read successfully")
                logMsgFile("Listened ",filename, listofLists.size-1)          // calling the log function to log that we listened to files
                val orders = listofLists.tail                                       // removing the header to make calculation
                val discountedOrders = orders.map { order =>                        // calling the discount orders, first we takes the internal lists with map function
                  val discount = applyDiscount(order, discountRules)                //applying the discount function, and giving it the order data and the rules
                  val finalPrice = calFinalPrice(order(4).toDouble, discount)       // calculating the final price to the new discount
                  order :+ discount.toString :+ finalPrice.toString                 // adding the discount and final price to data
                }

                Class.forName("com.mysql.cj.jdbc.Driver")                 //calling the driver to connect to database
                val jdbcUrl = "jdbc:mysql://localhost:3306/mydatabase"               // url of the database which is named mydatabase
                val jdbcUsername = "root"                                            //username
                val jdbcPassword = "snzboo123"                                       // password
                val connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword)     //opening the connection
                val tableName = "output_table"
                //inserting the data into output table in the database with the data we had
                val insertSql = s"INSERT INTO $tableName (timestamps, product_name, expiry_date, quantity, unit_price, channels, payment_method,  discount, final_price) VALUES (?, ?, ?, ?, ?, ?, ?,?,?)"
                val preparedStatement: PreparedStatement = connection.prepareStatement(insertSql)
                discountedOrders.foreach { order =>
                  try {
                    //putting the data into the insert statements
                    preparedStatement.setString(1, order.head)
                    preparedStatement.setString(2, order(1))
                    preparedStatement.setString(3, order(2))
                    preparedStatement.setInt(4, order(3).toInt)
                    preparedStatement.setDouble(5, order(4).toDouble)
                    preparedStatement.setString(6, order(5))
                    preparedStatement.setString(7, order(6))
                    preparedStatement.setDouble(8, order(7).toDouble)
                    preparedStatement.setDouble(9, order(8).toDouble)
                    preparedStatement.executeUpdate()
                  } catch {                                                   //catching the exception if reading file failed, so we can retry up to 3 times
                    case e: Exception =>
                      println(s"Failed to insert file $filename into database due to exception: ${e.getMessage}. Retrying in 5 seconds...")
                      Thread.sleep(5000)
                      retryCount += 1                                         //counting the number of retries
                  }
                }
                println(s"Processed orders from $filename in MySQL database successfully")
                logMsgFile("Processed",filename, orders.size)         //calling log file to put it into log file
                preparedStatement.close()
                connection.close()                                           //finished? close connection
                retry = false
              }
            } catch {
              case _: Exception =>
                println(s"Failed to read file $filename. Retrying in 5 seconds...")       //incase failed to read the file it self
                Thread.sleep(5000)
                retryCount += 1
            }
          }
          if (retryCount == 3) {
            println(s"Failed to read file $filename after $retryCount retries. Moving on to the next file.")     //if failed in the 3 tries
          } else {
            Files.delete(file.toPath)
            println(s"File $filename deleted successfully")           //deleting the file
          }
        }
      }
    }
    key.reset()
  }
 // defining the discount rule it self, which take the data, and the rules, and return the discount
  def applyDiscount(list: List[String], businessRules: List[DiscountRule]): Double = {
    //first in successfulRules, filtering all the data to return only the data that the first check function is valid
    val successfulRules = businessRules.filter(dataRow => dataRow.check(list))
    //now the successfulRules, will match the 3 cases
    successfulRules match {
      case Nil => 0.0                                     //in case empty
      case List(dataRow) => dataRow.applyDiscount(list)   //in case only 1 function succeed, then will take it's discount function that return double
      case _ =>                                           //incase more than one
        //first sorting the data by the applied discount , ascedning, then take right the highest 2 value, because we sorted ascending, then taking average
        val topTwoRules = successfulRules.sortBy(dataRow => dataRow.applyDiscount(list)).takeRight(2)
        (topTwoRules.head.applyDiscount(list) + topTwoRules(1).applyDiscount(list)) / 2.0
    }
  }
}