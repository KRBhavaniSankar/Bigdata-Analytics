  import org.joda.time.Months
  import org.joda.time.format.DateTimeFormat
  
  val startMonth = "202005"
  val endMonth = "202010"
  val originalFormat = DateTimeFormat.forPattern("yyyyMM")
  val parsedStartMonth = originalFormat.parseDateTime(startMonth)
  val parsedEndMonth =  originalFormat.parseDateTime(endMonth)
  val durationMonths = Months.monthsBetween(parsedStartMonth,parsedEndMonth).getMonths()

  (0 to durationMonths).map(i => {
  
    val currMonth = parsedStartMonth.plusMonths(i)
    val currMonthStartDate = currMonth.dayOfMonth().withMinimumValue().toString("yyyy-MM-dd") + " 00:00:00"
    val currMonthLastDate = currMonth.dayOfMonth().withMaximumValue().toString("yyyy-MM-dd") + " 23:59:59"
    val filterDF = inputDF.filter($"order_dt".between(currMonthStartDate,currMonthLastDate))
    
    println(currMonth.toString("YYYY-MM"),currMonthStartDate,currMonthLastDate,filterDF.count)
  })
