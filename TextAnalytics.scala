import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
//
object TextAnalytics {
    def getWordCount(sc:SparkContext, file:String) = {
      val lines = sc.textFile(file)
      val word_count = lines.flatMap(x => x.split(' ')).
      map(x => (x.toLowerCase().trim().stripSuffix(",").stripSuffix(".").replace("\u2019", "'"), 1)).
      reduceByKey((a, b) => a + b)
      //println(word_count.count())
      word_count
    }
  	//
  def main(args: Array[String]) {
  	println(new java.io.File( "." ).getCanonicalPath)
  	val conf = new SparkConf(false) // skip loading external settings
  	.setMaster("local") // could be "local[4]" for 4 threads
  	.setAppName("Chapter 9")
  	.set("spark.logConf", "false") //"true")
  	val sc = new SparkContext(conf) // ("local","Text Analytics) if using directly
  	sc.setLogLevel("WARN") // ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN
  	println(s"Running Spark Version ${sc.version}")
  	//
  	val word_count_bo = getWordCount(sc,"data/sotu/2009-2014-BO.txt")
    println("BO Raw Count = " + word_count_bo.count())
  	//
    val word_count_bo_2015 = getWordCount(sc,"data/sotu/2009-2015-BO.txt")
    println("BO Raw Count 2015 = " + word_count_bo_2015.count())
  	//
    val word_count_bo_2016 = getWordCount(sc,"data/sotu/2016-BO.txt")
    println("BO Raw Count 2016 = " + word_count_bo_2016.count())
  	//
    val word_count_gwb = getWordCount(sc,"data/sotu/2001-2008-GWB.txt")
    println("GWB Raw Count = " + word_count_gwb.count())
  	//
    val word_count_wjc = getWordCount(sc,"data/sotu/1994-2000-WJC.txt")
    println("WJC Raw Count = " + word_count_wjc.count())
  	//
    val word_count_jfk = getWordCount(sc,"data/sotu/1961-1963-JFK.txt")
    println("JFK Raw Count = " + word_count_jfk.count())
  	//
    val word_count_fdr = getWordCount(sc,"data/sotu/1934-1945-FDR.txt")
    println("FDR Raw Count = " + word_count_fdr.count())
  	//
    val word_count_al = getWordCount(sc,"data/sotu/1861-1864-AL.txt")
    println("AL Raw Count = " + word_count_al.count())
  	//
    val word_count_gw = getWordCount(sc,"data/sotu/1790-1796-GW.txt")
    println("GW Raw Count = " + word_count_gw.count())
    //
    word_count_bo.sortBy(_._2, ascending=false).take(5).foreach(println)
    // Yikes, we need to filter out the common words
    val common_words = List ("","us","has","all", "they", "from", "who","what","on","by","more","as","not","their","can",
                             "new","it","but","be","are","--","i","have","this","will","for","with","is","that","in",
                             "our","we","a","of","to","and","the","that's","or","make","do","you","at","it\'s","than",
                             "if","know","last","about","no","just","now","an","because","<p>we","why","we\'ll","how",
                             "two","also","every","come","we've","year","over","get","take","one","them","we\'re","need",
                             "want","when","like","most","-","been","first","where","so","these","they\'re","good","would",
                             "there","should","-->","<!--","up","i\'m","his","their","which","may","were","such","some",
                             "those","was","here","she","he","its","her","his","don\'t","i\'ve","what\'s","didn\'t",
                             "shouldn\'t","(applause.)","let\'s","doesn\'t","(laughter.)","any","other","america","american","americans","must")
    //This is my list. Would be useful for other projects as well. You can add your own
    val word_count_bo_clean = word_count_bo.filter(x => ! common_words.contains(x._1))
    println("BO Clean = " + word_count_bo_clean.count())
    //
    word_count_bo_clean.sortBy(_._2, ascending=false).take(10).foreach(println)
    //
    val word_count_bo_2015_clean = word_count_bo_2015.filter(x => ! common_words.contains(x._1))
    val word_count_bo_2016_clean = word_count_bo_2016.filter(x => ! common_words.contains(x._1))
    //
    val word_count_gwb_clean = word_count_gwb.filter(x => ! common_words.contains(x._1))
    word_count_gwb_clean.count()
    //
    val word_count_wjc_clean = word_count_wjc.filter(x => ! common_words.contains(x._1))
    println("*** WJC = " + word_count_wjc_clean.count())
    //
    val word_count_jfk_clean = word_count_jfk.filter(x => ! common_words.contains(x._1))
    println("*** JFK = " + word_count_jfk_clean.count())
    //
    val word_count_fdr_clean = word_count_fdr.filter(x => ! common_words.contains(x._1))
    println("*** FDR = " + word_count_fdr_clean.count())
    //
    val word_count_al_clean = word_count_al.filter(x => ! common_words.contains(x._1))
    println("*** AL = " + word_count_al_clean.count())
    //
    val word_count_gw_clean = word_count_gw.filter(x => ! common_words.contains(x._1))
    println("*** GW = " + word_count_gw_clean.count())
    //
    // GWB vs Lincoln
    println("*** GWB")
    word_count_gwb_clean.sortBy(_._2, ascending=false).take(10).foreach(println)
    println("*** Lincoln")
    word_count_al_clean.sortBy(_._2, ascending=false).take(10).foreach(println)
    //
    println("*** WJC")
    word_count_wjc_clean.sortBy(_._2, ascending=false).take(10).foreach(println)
    //
    println("*** FDR")
    word_count_fdr_clean.sortBy(_._2, ascending=false).take(10).foreach(println)
    //
    println("*** GW")
    word_count_gw_clean.sortBy(_._2, ascending=false).take(10).foreach(println)
    //
    // Has Barack Obama changed in 2015 ?
    //
    println("*** BO Diff 1")
    val bo_diff_1 = word_count_bo_2015_clean.subtractByKey(word_count_bo_clean)
    bo_diff_1.sortBy(_._2, ascending=false).take(10).foreach(println)
    //
    // What about 2016 ?
    //
    println("*** BO Diff 2")
    val bo_diff_2 = word_count_bo_2016_clean.subtractByKey(word_count_bo_2015_clean)
    bo_diff_2.sortBy(_._2, ascending=false).take(10).foreach(println)
    //
    // What mood was the country in 1790-1796 vs 2009-2015 ?
    //
    println("*** BO-GW")
    val bo_gw = word_count_bo_clean.subtractByKey(word_count_gw_clean)
    bo_gw.sortBy(_._2, ascending=false).take(10).foreach(println)
    //
    println("*** GW-BO")
    val gw_bo = word_count_gw_clean.subtractByKey(word_count_bo_clean)
    gw_bo.sortBy(_._2, ascending=false).take(10).foreach(println)
    //
    println("*** GWB-AL")
    val gwb_al = word_count_gwb_clean.subtractByKey(word_count_al_clean)
    gwb_al.sortBy(_._2, ascending=false).take(10).foreach(println)
    //
    // Now it is easy to see Obama vs. FDR or WJC vs. AL …
    //
    println("** That's All Folks ! **")
    // Oh, wait - One More/Last Thing
    // The debates !!!
    val rd = getWordCount(sc,"data/sotu/debates/RepublicanDebate-2016.txt")
    println("Repub. Debate = " + rd.count())
    val rd_clean = rd.filter(x => ! common_words.contains(x._1))
    println("RD Clean = " + rd_clean.count())
    //
    val dd = getWordCount(sc,"data/sotu/debates/DemocraticDebate-2016.txt")
    println("Dem. Debate = " + dd.count())
    val dd_clean = dd.filter(x => ! common_words.contains(x._1))
    println("DD Clean = " + dd_clean.count())
    //
    dd_clean.sortBy(_._2, ascending=false).take(10).foreach(println)
    rd_clean.sortBy(_._2, ascending=false).take(10).foreach(println)    
    //
    println("*** RD-DD")
    rd_clean.subtractByKey(dd_clean).sortBy(_._2, ascending=false).take(10).foreach(println)
    //
    println("*** DD-RD")
    dd_clean.subtractByKey(rd_clean).sortBy(_._2, ascending=false).take(10).foreach(println)
    //
    val common2 = List("sanders:","clinton:","clinton","sanders","cooper:","o'malley:","o'malley","holt:","todd:","ramos:","maddow:",
    "mitchell:","salinas:","webb:","woodruff:","[through","lemon:","ifill:","webb","andrea","chafee:","trump:","trump","rubio:","cruz:",
    "tapper:","bush:","kasich:","blitzer:","kelly:","kasich","carson","baier:","wallace:","carson:","paul:","christie:","fiorina:",
    "bartiromo:","christie","hewitt:","cavuto:","jeb","[applause]","me","your","my","\u2014","said","out","very","huckabee:","—","quintanilla:",
    "harwood:","jake","ben","hugh","baker:","huckabee","walker:","trump?","rand","rubio?","megyn","cruz?","tumulty:","sanders?","chafee",
    "translator]:","lester","martin")
    //
    val dd_1 = dd_clean.filter(x => ! common2.contains(x._1))
    val rd_1 = rd_clean.filter(x => ! common2.contains(x._1))
    //
    println("*** RD-DD")
    rd_1.subtractByKey(dd_1).sortBy(_._2, ascending=false).take(15).foreach(println)
    //
    println("*** DD-RD")
    dd_1.subtractByKey(rd_1).sortBy(_._2, ascending=false).take(15).foreach(println)
    //
    println("** Done **")
  }
}