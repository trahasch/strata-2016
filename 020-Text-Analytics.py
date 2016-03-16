
# coding: utf-8

# ## Text Analytics
# ###In which we analyze the Mood of the nation from inferences on SOTU by the POTUS
# ###   (State Of The Union addresses by the President Of The US)
# 
# #### Goal is to find interesting words in the speeches that reflect the times.
# 
# ####Am sure Lincoln didn't worry about WMDs and Iraq; neither did George Washington about inflation, Wall Street and Jobs.

# In[1]:

from pyspark.context import SparkContext
print "Running Spark Version %s" % (sc.version)


# In[2]:

from pyspark.conf import SparkConf
conf = SparkConf()
print conf.toDebugString()


# ###MapReduce in one line !
# ###1. Split lines into words on space
# ###2. Create key-value pair with key=word, value = 1
# ###3. Sum value for each word (er ... key)
# ###4. Then we get key-value RDD with key=word and value = number of times the word occured in a document

# In[3]:

# imports
from operator import add


# In[4]:

lines = sc.textFile("sotu/2009-2014-BO.txt")
word_count_bo = lines.flatMap(lambda x: x.split(' ')).    map(lambda x: (x.lower().rstrip().lstrip().rstrip(',').rstrip('.'), 1)).    reduceByKey(add)
word_count_bo.count()
#6658 without lower, 6299 with lower, rstrip,lstrip 4835


# In[5]:

lines = sc.textFile("sotu/2009-2015-BO.txt")
word_count_bo_2015 = lines.flatMap(lambda x: x.split(' ')).    map(lambda x: (x.lower().rstrip().lstrip().rstrip(',').rstrip('.').replace(u"\u2019", "'"), 1)).    reduceByKey(add)
word_count_bo_2015.count()


# In[6]:

#output = word_count_bo.collect()
#for (word, count) in output:
#    print "%s: %i" % (word, count)


# In[7]:

lines = sc.textFile("sotu/2001-2008-GWB.txt")
word_count_gwb = lines.flatMap(lambda x: x.split(' ')).map(lambda x: (x.lower().rstrip().lstrip().rstrip(',').rstrip('.'), 1)).reduceByKey(add)
word_count_gwb.count()


# In[8]:

lines = sc.textFile("sotu/1994-2000-WJC.txt")
word_count_wjc = lines.flatMap(lambda x: x.split(' ')).map(lambda x: (x.lower().rstrip().lstrip().rstrip(',').rstrip('.'), 1)).reduceByKey(add)
word_count_wjc.count()


# In[9]:

lines = sc.textFile("sotu/1961-1963-JFK.txt")
word_count_jfk = lines.flatMap(lambda x: x.split(' ')).map(lambda x: (x.lower().rstrip().lstrip().rstrip(',').rstrip('.'), 1)).reduceByKey(add)
word_count_jfk.count()


# In[10]:

lines = sc.textFile("sotu/1934-1945-FDR.txt")
word_count_fdr = lines.flatMap(lambda x: x.split(' ')).map(lambda x: (x.lower().rstrip().lstrip().rstrip(',').rstrip('.'), 1)).reduceByKey(add)
word_count_fdr.count()


# In[11]:

lines = sc.textFile("sotu/1861-1864-AL.txt")
word_count_al = lines.flatMap(lambda x: x.split(' ')).map(lambda x: (x.lower().rstrip().lstrip().rstrip(',').rstrip('.'), 1)).reduceByKey(add)
word_count_al.count()


# In[12]:

lines = sc.textFile("sotu/1790-1796-GW.txt")
word_count_gw = lines.flatMap(lambda x: x.split(' ')).map(lambda x: (x.lower().rstrip().lstrip().rstrip(',').rstrip('.'), 1)).reduceByKey(add)
word_count_gw.count()


# In[13]:

common_words = ["","us","has","all", "they", "from", "who","what","on","by","more","as","not","their","can",
                             "new","it","but","be","are","--","i","have","this","will","for","with","is","that","in",
                             "our","we","a","of","to","and","the","that's","or","make","do","you","at","it\'s","than",
                             "if","know","last","about","no","just","now","an","because","<p>we","why","we\'ll","how",
                             "two","also","every","come","we've","year","over","get","take","one","them","we\'re","need",
                             "want","when","like","most","-","been","first","where","so","these","they\'re","good","would",
                             "there","should","-->","<!--","up","i\'m","his","their","which","may","were","such","some",
                             "those","was","here","she","he","its","her","his","don\'t","i\'ve","what\'s","didn\'t",
                             "shouldn\'t","(applause.)","let\'s","doesn\'t","(laughter.)"]


# In[14]:

word_count_bo_1 = word_count_bo.sortBy(lambda x: x[1],ascending=False)


# In[15]:

for x in word_count_bo_1.take(10):
    print x


# In[16]:

word_count_bo_clean = word_count_bo_1.filter(lambda x: x[0] not in common_words)


# In[17]:

word_count_bo_clean.count()


# In[18]:

for x in word_count_bo_clean.take(20):
    print x


# In[19]:

word_count_bo_2015_clean = word_count_bo_2015.filter(lambda x: x[0] not in common_words)


# In[20]:

word_count_gwb_clean = word_count_gwb.filter(lambda x: x[0] not in common_words)
word_count_gwb_clean.count()
for x in word_count_gwb_clean.sortBy(lambda x: x[1],ascending=False).take(15):
    print x


# In[21]:

word_count_wjc_clean = word_count_wjc.filter(lambda x: x[0] not in common_words)
word_count_wjc_clean.count()
for x in word_count_wjc_clean.sortBy(lambda x: x[1],ascending=False).take(15):
    print x


# In[22]:

word_count_jfk_clean = word_count_wjc.filter(lambda x: x[0] not in common_words)
word_count_jfk_clean.count()
for x in word_count_jfk_clean.sortBy(lambda x: x[1],ascending=False).take(15):
    print x


# In[23]:

word_count_fdr_clean = word_count_fdr.filter(lambda x: x[0] not in common_words)
word_count_fdr_clean.count()
for x in word_count_fdr_clean.sortBy(lambda x: x[1],ascending=False).take(15):
    print x


# In[24]:

word_count_al_clean = word_count_al.filter(lambda x: x[0] not in common_words)
word_count_al_clean.count()
for x in word_count_al_clean.sortBy(lambda x: x[1],ascending=False).take(15):
    print x


# In[25]:

word_count_gw_clean = word_count_gw.filter(lambda x: x[0] not in common_words)
word_count_gw_clean.count()
for x in word_count_gw_clean.sortBy(lambda x: x[1],ascending=False).take(15):
    print x


# ## Has Barack Obama changed in 2015 ?
# ### As reflected in the SOTU 2009-2015 vs SOTU 2009-2014 ?

# In[26]:

for x in word_count_bo_2015_clean.subtractByKey(word_count_bo_clean).sortBy(lambda x: x[1],ascending=False).take(15):
    print x


# #Coding Exercise
# ## What mood was the country in 1790-1796 vs 2009-2015 ?
# ### Hint:
# ### 1. word_count_gw_clean = 1790-1796-GW.txt
# ### 2. word_count_bo_2015_clean

# In[27]:

for x in word_count_bo_clean.subtractByKey(word_count_gw_clean).sortBy(lambda x: x[1],ascending=False).take(15):
    print x


# In[28]:

for x in word_count_gw_clean.subtractByKey(word_count_bo_clean).sortBy(lambda x: x[1],ascending=False).take(15):
    print x


# ###Now it is easy to see Obama vs. FDR or WJC vs. AL ...

# In[29]:

for x in word_count_fdr_clean.subtractByKey(word_count_bo_clean).sortBy(lambda x: x[1],ascending=False).take(15):
    print x


# In[30]:

for x in word_count_bo_clean.subtractByKey(word_count_fdr_clean).sortBy(lambda x: x[1],ascending=False).take(15):
    print x


# In[31]:

for x in word_count_bo_clean.subtractByKey(word_count_wjc_clean).sortBy(lambda x: x[1],ascending=False).take(15):
    print x


# In[32]:

for x in word_count_bo_clean.subtractByKey(word_count_gwb_clean).sortBy(lambda x: x[1],ascending=False).take(15):
    print x


# In[33]:

for x in word_count_bo_clean.subtractByKey(word_count_al_clean).sortBy(lambda x: x[1],ascending=False).take(15): #collect():
    print x


# In[34]:

for x in word_count_wjc_clean.subtractByKey(word_count_al_clean).sortBy(lambda x: x[1],ascending=False).take(15): #collect():
    print x


# In[35]:

for x in word_count_gwb_clean.subtractByKey(word_count_al_clean).sortBy(lambda x: x[1],ascending=False).take(15): #collect():
    print x


# In[36]:

for x in word_count_al_clean.subtractByKey(word_count_bo_clean).sortBy(lambda x: x[1],ascending=False).take(15): #collect():
    print x


# In[37]:

for x in word_count_al_clean.subtractByKey(word_count_wjc_clean).sortBy(lambda x: x[1],ascending=False).take(15): #collect():
    print x


# ## That is All Folks !

# In[ ]:



